//! Welcome to the ADS client library.
//! 
//! This create enables communication over the [Beckhoff ADS](https://infosys.beckhoff.com/content/1033/tcinfosys3/11291871243.html) protocoll.
//! 
//! The ADS client is used to work beside a 
//! [TC1000 ADS router](https://www.beckhoff.com/en-en/products/automation/twincat/tc1xxx-twincat-3-base/tc1000.html)
//! which is part of every TwinCAT installation. The client requires at least TwinCAT Version 3.1.4024.x.
//! 
//! This crate grants access to the following ADS commands:
//! 
//! - [Client::read_state]
//! - [Client::read]
//! - [Client::write]
//! - [Client::read_write]
//! - [Client::write_control]
//! - [Client::add_device_notification]
//! - [Client::delete_device_notification]
//! - [Client::read_device_info]
//! 
//! The methods are implemented asynchronous and non-blocking based on the [tokio](https://tokio.rs/) runtime.
//! 
//! # Usage
//! 
//! Checkout the [example section](https://github.com/hANSIc99/ads_client/tree/main/examples) in the repsoitory.

#![allow(unused)]

#[macro_use]
mod misc;
mod ads_read;
mod ads_write;
mod ads_read_state;
mod ads_read_write;
mod ads_add_device_notification;
mod ads_delete_device_notification;
mod ads_write_control;
mod ads_read_device_info;

use std::time::{Instant, Duration};
use std::io;
use std::net::Ipv4Addr;
use std::mem::size_of_val;
use std::sync::{Arc, atomic::{AtomicU16, Ordering}};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{Mutex, oneshot};
use tokio::{runtime, stream};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::time::sleep;
use log::{trace, debug, info, warn, error};
use bytes::{Bytes, BytesMut};


use misc::{AdsCommand, CommandReadHandle, CommandWriteHandle, HandleData, NotHandle, AdsStampHeader, AdsNotificationSample};
pub use misc::{AdsTimeout, AmsNetId, AmsPort, AmsAddr, AdsNotificationAttrib, AdsTransMode, StateInfo, DeviceStateInfo, AdsState, Notification, Result, AdsError, AdsErrorCode}; // Re-export type

/// Size of the AMS/TCP + ADS headers
// https://infosys.beckhoff.com/content/1033/tc3_ads_intro/115845259.html?id=6032227753916597086
const HEADER_SIZE           : usize = 38;
const AMS_HEADER_SIZE       : usize = HEADER_SIZE - 6; // without leading nulls and length
const LEN_READ_REQ          : usize = 12;
const LEN_RW_REQ_MIN        : usize = 16;
const LEN_W_REQ_MIN         : usize = 12;
const LEN_ADD_DEV_NOT       : usize = 38;
const LEN_STAMP_HEADER_MIN  : usize = 12;   // Time Stamp [8] + No Samples [4]
const LEN_NOT_SAMPLE_MIN    : usize = 8;    // Notification Handle [4] + Sample Size [4]
const LEN_DEL_DEV_NOT       : usize = 4;
const LEN_WR_CTRL_MIN       : usize = 8;

enum ProcessStateMachine{
    ReadHeader,
    ReadPayload { len_payload: usize, err_code: u32, invoke_id: u32, cmd: AdsCommand}
}

#[derive(Debug, Clone)]
pub struct ClientBuilder<RouterAddr> {
    router_addr: RouterAddr,
    dst_ams_addr: AmsAddr,
    src_ams_addr: Option<AmsAddr>,
    timeout: AdsTimeout,
    retry_delay: Option<Duration>,
}

impl ClientBuilder<()> {
    pub fn new(dst_ams_addr: AmsAddr) -> ClientBuilder<(Ipv4Addr, u16)> {
        ClientBuilder {
            router_addr: (Ipv4Addr::new(127, 0, 0, 1), 48898),
            dst_ams_addr,
            src_ams_addr: Default::default(),
            timeout: Default::default(),
            retry_delay: Default::default(),
        }
    }
}

impl<RouterAddr> ClientBuilder<RouterAddr> {
    pub fn router_addr<NextRouterAddr: ToSocketAddrs>(self, router_addr: NextRouterAddr) -> ClientBuilder<NextRouterAddr> {
        ClientBuilder {
            router_addr,
            dst_ams_addr: self.dst_ams_addr,
            src_ams_addr: self.src_ams_addr,
            timeout: self.timeout,
            retry_delay: self.retry_delay
         }
    }
}

impl<RouterAddr> ClientBuilder<RouterAddr> {
    pub fn src_ams_addr(mut self, src_ams_addr: AmsAddr) -> Self {
        self.src_ams_addr = Some(src_ams_addr);
        self
    }

    pub fn timeout(mut self, timeout: AdsTimeout) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn retry_delay(mut self, retry_delay: Option<Duration>) -> Self {
        self.retry_delay = retry_delay;
        self
    }
}

impl<RouterAddr: ToSocketAddrs> ClientBuilder<RouterAddr> {
    pub async fn build(self) -> Result<Client> {
        Client::new(
            self.router_addr,
            self.dst_ams_addr,
            self.src_ams_addr,
            self.timeout,
            self.retry_delay,
        ).await
    }
}

/// An ADS client to use in combination with the [TC1000 ADS router](https://www.beckhoff.com/en-en/products/automation/twincat/tc1xxx-twincat-3-base/tc1000.html).
/// 
/// The client opens a port on the local ADS router in order to submit ADS requests.
/// Use the [Client::new] method to create an instance.
#[derive(Debug)]
pub struct Client {
    dst_ams_addr    : AmsAddr,
    src_ams_addr    : AmsAddr,
    timeout         : u64, // ADS Timeout [s]
    socket_wrt      : Arc<Mutex<WriteHalf<TcpStream>>>,
    cmd_handles     : Arc<Mutex<Vec<CommandWriteHandle>>>, // Internal stack of Handles (^=ADS CommandsInvoke) to write responses
    not_handles     : Arc<Mutex<Vec<NotHandle>>>,
    ams_header      : [u8; HEADER_SIZE],
    hdl_cnt         : Arc<AtomicU16>
}

// TODO: Implement Defaul trait
// https://doc.rust-lang.org/std/default/trait.Default.html


impl Client {
   
    async fn connect(
        router_addr: impl ToSocketAddrs,
        dst_ams_addr: &AmsAddr,
        src_ams_addr: &Option<AmsAddr>,
    ) -> Result<(TcpStream, AmsAddr)> {
        let mut stream  = TcpStream::connect(router_addr).await?;

        // If we are given a source AMS address, we don't need to request one from the ADS router.
        if let Some(src_ams_addr) = src_ams_addr {
            return Ok((stream, src_ams_addr.clone()));
        };

        // Otherwise request a source AMS address from the ADS router...

        let handshake : [u8; 8] = [0x00, 0x10, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00 ];
        let mut answer : [u8; 14] = [0; 14];

        if let Err(error) = stream.write_all(&handshake).await {
            error!("Failed to write to socket");
            return Err(error.into());
        };

        match stream.read_exact(&mut answer).await {
            Ok(n) => {
                info!("Connection to AMS router established");

                let src_ams_net_id = AmsNetId([answer[6], answer[7], answer[8], answer[9], answer[10], answer[11]]);
                let src_ams_port = u16::from_ne_bytes(answer[12..14].try_into().expect("Parsing source port failed"));

                Ok((stream, (src_ams_net_id, src_ams_port)))
            },
            Err(_error) => {
                error!("Router port disabled – TwinCAT system service not started.");
                Err(AdsError{n_error : 18, s_msg : String::from("Port disabled – TwinCAT system service not started.")})
            }
        }
    } 

    async fn process_response(cmd_handles: Arc<Mutex<Vec<CommandWriteHandle>>>, not_handles: Arc<Mutex<Vec<NotHandle>>>, mut rd_stream : ReadHalf<TcpStream>, retry_delay: Option<Duration>) {
        
        let mut state = ProcessStateMachine::ReadHeader;
        let rt = runtime::Handle::current();
        
        loop {
            match &mut state {

                ProcessStateMachine::ReadHeader => {

                    let mut header_buf : [u8; HEADER_SIZE] = [0; HEADER_SIZE];

                    match rd_stream.read_exact(&mut header_buf).await {
                        Ok(0) => {
                           warn!("[0] Incoming ADS response - no bytes to read");
                        }
                        Ok(_) => {
                            let len_payload = Client::extract_length(&header_buf).unwrap_or_default();
                            let err_code = Client::extract_error_code(&header_buf).unwrap_or_default();
                            let invoke_id   = Client::extract_invoke_id(&header_buf).unwrap_or_default();
                            let ads_cmd     = Client::extract_cmd_tyte(&header_buf).unwrap_or_default();

                            if(len_payload == 0){
                                warn!("Invoke id {}: No ADS payload available - skip", invoke_id);
                                continue;
                            }

                            trace!("[0] Incoming ADS response with {:?} byte payload", len_payload);

                            state = ProcessStateMachine::ReadPayload{
                                len_payload : len_payload,
                                err_code    : err_code,
                                invoke_id   : invoke_id,
                                cmd         : ads_cmd
                            };

                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            warn!("TcpStream: false positive reaction / stream was not yet ready for reading{:?}", e);
                            continue;
                        }
                        Err(e) => {
                            error!("Socket Error (0x1): {:?}", e);
                            //panic!("Socket Error (0x1): {:?}", e);
                            if let Some(ref delay) = retry_delay {
                                sleep(*delay).await;
                            }
                        }
                    }
                }
                
                ProcessStateMachine::ReadPayload {len_payload, err_code, invoke_id, cmd} => {
                    
                    let mut payload = BytesMut::zeroed(*len_payload);

                    match rd_stream.read_exact(&mut payload[..]).await {
                        Ok(0) => {
                            info!("[1] ADS response {:?}, Invoke ID: {:?}: - zero payload", cmd, invoke_id);
                            state = ProcessStateMachine::ReadHeader;
                        }
                        Ok(_) => {
                            
                            let buf = payload.freeze(); // Convert to Bytes
                            match cmd {
                                AdsCommand::DeviceNotification => {
                                    trace!("[1] Processing device notification");
                                    let _not_handles = Arc::clone(&not_handles); 
                                    rt.spawn(Client::process_device_notification(_not_handles, buf));

                                },
                                _ => {
                                    trace!("[1] Processing ADS response");
                                    let _handles = Arc::clone(&cmd_handles);
                                    rt.spawn(Client::process_command(*err_code, *invoke_id, _handles, buf));
                                }

                            };

                            state = ProcessStateMachine::ReadHeader;
                        }
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            warn!("ADS command {:?}, Invoke ID: {:?}: - WouldBlock error during reading occured", cmd, invoke_id);
                            continue;
                        }
                        Err(e) => {
                            error!("ADS command {:?}, Invoke ID: {:?}: - Error occurred: {:?}", cmd, invoke_id, e);
                            //panic!("Socket Error (0x1): {:?}", e);
                            if let Some(ref delay) = retry_delay {
                                sleep(*delay).await;
                            }
                        }
                    } // match
                }
            } // match
        } // loop
    } // fn

    async fn socket_write(&self, data: &[u8] ) -> Result<()> {
        let mut stream = self.socket_wrt.lock().await;
        
        if let Err(_) = stream.write_all(data).await {
            Err( AdsError { n_error : 10, s_msg : String::from("Writing to Tcp Stream socket failed") } )
        } else {
            Ok(())
        }
    }
    
    /// Create a new instance of an ADS client.
    /// 
    /// - `addr` AmsNetId of the target system
    /// - `port` ADS port number to communicate with
    /// - `timeout` Value for ADS timeout value ([AdsTimeout::DefaultTimeout] corresponds to 5s)
    /// 
    /// # Example
    /// ```rust
    /// use ads_client::{ClientBuilder, Result};
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let ads_client =  ClientBuilder::new("5.80.201.232.1.1", 851).build().await?;
    ///     Ok(())
    /// }
    /// ```
    async fn new(
        router_addr: impl ToSocketAddrs,
        dst_ams_addr: AmsAddr,
        src_ams_addr: Option<AmsAddr>,
        timeout: AdsTimeout,
        retry_delay: Option<Duration>,
    ) -> Result<Self> {
        let state_flag : u16 = 4;
        let error_code : u32 = 0;

        let timeout = match timeout {
            AdsTimeout::DefaultTimeout => 5,
            AdsTimeout::CustomTimeout(time) => time
        };

        let hdl_rt = runtime::Handle::current();

        let (stream, src_ams_addr) = Client::connect(router_addr, &dst_ams_addr, &src_ams_addr).await?;

        let (dst_ams_net_id, dst_ams_port) = &dst_ams_addr;
        let (src_ams_net_id, src_ams_port) = &src_ams_addr;

        info!("ADS client port opened: {}", src_ams_port);

        // Split the stream into a read and write part
        //
        // Read-half goes to process_response()
        // Write-half goes to Self

        let (read, write) = tokio::io::split(stream);

        let a_socket_wrt = Arc::new(Mutex::new(write));

        // Create atomic instances of the handle vector
        let a_cmd_handles = Arc::new(Mutex::new( Vec::<CommandWriteHandle>::new() ));
        let a_not_handles =  Arc::new(Mutex::new( Vec::<NotHandle>::new() ));

        // Process incoming ADS responses
        let response_vector_a  = Arc::clone(&a_cmd_handles);
        let not_response_vector_a = Arc::clone(&a_not_handles);
        hdl_rt.spawn(Client::process_response(response_vector_a, not_response_vector_a, read, retry_delay));

        let ams_header = [
            0, // Reserved
            0,
            0, // Header size + playload
            0,
            0,
            0,
            dst_ams_net_id[0], // Target NetId
            dst_ams_net_id[1],
            dst_ams_net_id[2],
            dst_ams_net_id[3],
            dst_ams_net_id[4],
            dst_ams_net_id[5],
            u16_low_byte!(*dst_ams_port), // Target port
            u16_high_byte!(*dst_ams_port), 
            src_ams_net_id[0], //  Source NetId
            src_ams_net_id[1],
            src_ams_net_id[2],
            src_ams_net_id[3],
            src_ams_net_id[4],
            src_ams_net_id[5],
            u16_low_byte!(*src_ams_port), // Source Port
            u16_high_byte!(*src_ams_port), 
            0, // Command-Id
            0, 
            u16_low_byte!(state_flag), // State flags
            u16_high_byte!(state_flag), 
            0, // Length
            0,
            0,
            0, 
            u32_lw_lb!(error_code), // Error code
            u32_lw_hb!(error_code),
            u32_hw_lb!(error_code),
            u32_hw_hb!(error_code), 
            0, // Invoke Id
            0,
            0,
            0
        ];

        Ok(Self {
            src_ams_addr,
            dst_ams_addr,
            timeout      : timeout,
            socket_wrt   : a_socket_wrt,
            cmd_handles  : a_cmd_handles,
            not_handles  : a_not_handles,
            ams_header   : ams_header,
            hdl_cnt      : Arc::new(AtomicU16::new(1))
        })
    }

    async fn register_command_handle(&self, invoke_id : u32, cmd : AdsCommand) -> CommandReadHandle {
        let (sender, receiver) = oneshot::channel();
        
        let cmd_write_handle = CommandWriteHandle {
            cmd_type    : cmd,
            invoke_id   : invoke_id,
            data_sender : sender,
        };
        let cmd_read_handle = CommandReadHandle {
            cmd_type        : cmd,
            invoke_id       : invoke_id,
            data_receiver   : receiver,
        };
    
        {
            let mut cmd_handles = self.cmd_handles.lock().await;
            cmd_handles.push(cmd_write_handle);
        }

        cmd_read_handle
    }

    async fn register_not_handle(&self, not_hdl: u32, callback: Notification) {
        let not_hdl = NotHandle {
            callback  : callback,
            not_hdl   : not_hdl,
        };

        {
            let mut not_handles = self.not_handles.lock().await;
            not_handles.push(not_hdl);
        }
    }

    fn create_invoke_id(&self) -> u32 {
        u32::from(self.hdl_cnt.fetch_add(1, Ordering::SeqCst))
    }

    fn c_init_ams_header(&self, invoke_id : u32, length_payload : Option<u32>, cmd : AdsCommand) -> [u8; HEADER_SIZE] {
        let length_payload = length_payload.unwrap_or(0);
        let length_header : u32 = AMS_HEADER_SIZE as u32 + length_payload;

        let mut ams_header : [u8; HEADER_SIZE] = self.ams_header;
        // length header + payload
        ams_header[2..6].copy_from_slice(&length_header.to_ne_bytes());
        // command id
        ams_header[22..24].copy_from_slice(&(cmd as u16).to_ne_bytes());
        // length payload
        ams_header[26..30].copy_from_slice(&length_payload.to_ne_bytes());
        // invoke Id
        ams_header[34..38].copy_from_slice(&invoke_id.to_ne_bytes());

        ams_header
    }

    fn eval_return_code(answer: &[u8]) -> Result<u32> {
        let ret_code = u32::from_ne_bytes(answer[0..4].try_into()?);

        if ret_code != 0 {
            Err(AdsError{ n_error : ret_code, s_msg : String::from("Errorcode of ADS response") }) // TODO Add text to error codes
        } else {
            Ok(ret_code)
        }
    }

    fn eval_ams_error(ams_err : u32) -> Result<()> {
        if ams_err != 0 {
            return Err(AdsError{n_error : ams_err, s_msg : String::from("Errorcode of ADS response") });
        }
        Ok(())
    }

    fn extract_error_code(answer: &[u8]) -> Result<u32> {
        Ok(u32::from_ne_bytes(answer[HEADER_SIZE-8..HEADER_SIZE-4].try_into()?))
    }

    fn extract_invoke_id(answer: &[u8]) -> Result<u32> {
        Ok(u32::from_ne_bytes(answer[HEADER_SIZE-4..HEADER_SIZE].try_into()?))
    }

    fn extract_cmd_tyte(answer: &[u8]) -> Result<AdsCommand>{
        u16::from_ne_bytes(answer[HEADER_SIZE-16..HEADER_SIZE-14].try_into()?).try_into()
    }

    fn extract_length(answer: &[u8]) -> Result<usize>{
        // length in AMS-Header https://infosys.beckhoff.com/content/1031/tc3_ads_intro/115847307.html
        let tmp = u32::from_ne_bytes(answer[HEADER_SIZE-12..HEADER_SIZE-8].try_into()?);
        //Err(AdsError{s_msg: String::from("test"),  n_error : 1212}) // DEBUG
        Ok(usize::try_from(tmp)?)
    }

    fn not_extract_length(answer: &[u8]) -> Result<usize>{
        let tmp = u32::from_ne_bytes(answer[0..4].try_into()?);
        Ok(usize::try_from(tmp)?)
    }

    /// Panics if the input slice is less than 8 bytes
    fn not_extract_stamps(answer: &[u8]) -> Result<u32>{
        Ok(u32::from_ne_bytes(answer[4..8].try_into()?))
    }

    async fn process_command(err_code: u32, invoke_id: u32, cmd_register: Arc<Mutex<Vec<CommandWriteHandle>>>, data: Bytes){
        trace!("[2] AdsCmd: Invoke ID: {}", invoke_id);

        let mut h = cmd_register.lock().await;
        if let Some(index) = h.iter().position(|hdl| hdl.invoke_id == invoke_id) {
            let hdl = h.swap_remove(index);
            hdl.write(HandleData { ams_err: err_code, payload: data });
        } else {
            warn!("No corresponding invoke ID found in CMD register - response will expire");
        }
    }

    async fn process_device_notification(not_register: Arc<Mutex<Vec<NotHandle>>>, data: Bytes){
        trace!("[2] Start processing AdsDeviceNotification");
        let stream_length = match Client::not_extract_length(&data){
            Ok(size) => size,
            Err(e) => {
                error!("Failed to extract notification length - Notification dropped - {:?}", e);
                return;
            }
        };

        let no_stamps = match Client::not_extract_stamps(&data){
            Ok(stamps) => stamps,
            Err(e) => {
                error!("Failed to extract number of stamps- Notification dropped - {:?}", e);
                return;
            }
        };

        let rt          = runtime::Handle::current();
        // Maximum stamp_header_offset == stream_size - sizeof(stamps)
        // ^= stream_size - 4
        
        // Calculate the last byte index of the AdsNotificaionStream (Length, Samples + AdsStampHeader)
        let max_stamp_header_offset = stream_length + size_of_val(&no_stamps); 
        let mut stamp_header_offset : usize = 8; // Start index of AdsNotificationStream


        for _ in 0..no_stamps { // Iterate over AdsStampHeader 
            // Return if there is no data beside of the AdsStampHeader consisting of time stamp [8] and no samples [4]
            if (stamp_header_offset + LEN_STAMP_HEADER_MIN) > max_stamp_header_offset {
                info!("Received Device Notification without sample data");
                continue;
            }
           
            
            let stamp_header = AdsStampHeader {
                timestamp : u64::from_ne_bytes(data[stamp_header_offset.. stamp_header_offset + 8]
                                                .try_into()
                                                .unwrap_or_default()),

                samples : u32::from_ne_bytes(data[stamp_header_offset + 8..stamp_header_offset + 12]
                                                .try_into()
                                                .unwrap_or_default())
            };

            if (stamp_header == AdsStampHeader::default()){
                info!("Empty AdsStampHeader - Continue with next stamp");
                continue;
            }

            // Increase stamp header offset, move it to first AdsNotificaionSample (+= 12 byte)
            stamp_header_offset += LEN_STAMP_HEADER_MIN;
            // == 20 (after first call)

            for _ in 0..stamp_header.samples {
                // Return if there is not enough data
                if (stamp_header_offset + LEN_NOT_SAMPLE_MIN) > max_stamp_header_offset {
                    info!("[A] Not enough data in available in stream");
                    return;
                }

                let not_sample = AdsNotificationSample {
                    not_hdl : u32::from_ne_bytes(data[stamp_header_offset..stamp_header_offset + 4]
                                                        .try_into()
                                                        .unwrap_or_default()),

                    sample_size : u32::from_ne_bytes(data[stamp_header_offset + 4 ..stamp_header_offset + 8]
                                                        .try_into()
                                                        .unwrap_or_default())
                };

                if (not_sample == AdsNotificationSample::default()){
                    info!("No data in AdsNotificationSample - skip");
                    continue;
                }

                stamp_header_offset += LEN_NOT_SAMPLE_MIN;

                if (stamp_header_offset + not_sample.sample_size as usize) > max_stamp_header_offset {
                    info!("[B] Not enough data in available in stream");
                    return;
                }

                let mut _cb : Option<Notification> = None;
                
                // The callback must be called after the lock. 
                // If it is called during the lock, it could block the access to the notification handles infinitely.

                { // LOCK
                    let mut _not_handles = not_register.lock().await;
                    let mut _iter = _not_handles.iter_mut();
                    
                    _cb = _iter.find( | hdl | hdl.not_hdl  == not_sample.not_hdl)
                            .and_then(| hdl : &mut NotHandle | Some( hdl.callback.clone() ) ); // Return callback and user data
                } // UNLOCK
                
                
                _cb.and_then(|callback| {
                    let payload = Bytes::from(data.slice(stamp_header_offset..stamp_header_offset + not_sample.sample_size as usize));
                    // let n_cnt = u16::from_ne_bytes(payload[..].try_into().expect("Failed to parse data")); // DEBUG

                    Some(
                            rt.spawn(async move  {
                            callback.call(not_sample.not_hdl, stamp_header.timestamp, payload);
                        })
                    )
                    
                }); // Process join handles?

                stamp_header_offset += not_sample.sample_size as usize;
            } // for idx_notification_sample in 0..stamp_header.samples
        } // for idx_stamp_header in 0..stamps
    }
}