use std::{net::{IpAddr, SocketAddr}, process::ExitCode, sync::Arc, time::Duration, u16};

use anyhow::{anyhow, Error};
use argh::FromArgs;
use rand::random;
use serde_json::json;
use tokio::{join, net::lookup_host, signal::unix::{signal, SignalKind}, sync::broadcast, time::sleep};

const EXIT_CODE_HOST_UP: u8 = 0;
const EXIT_CODE_HOST_DOWN: u8 = 8;
const EXIT_CODE_ERROR: u8 = 1;

#[derive(Clone, Debug)]
enum LookupTaskCommands {
    Lookup,
    Exit
}

#[derive(Clone, Debug)]
enum LookupResult {
    Success(IpAddr, Arc<String>),
    Failure(Arc<Error>, Arc<String>)
}

#[derive(FromArgs)]
/// Ping with frequent name resolution
struct Args {
    /// host to ping
    #[argh(positional)]
    host: String,

    /// number of times to ping; omit to ping forever
    #[argh(option, short = 'c', long = "count")]
    count: Option<u128>,

    /// sequence count before resolving hostname again
    #[argh(option, short = 'r', long = "resolve", default = "get_default_dns_renew()")]
    resolve_cycles: u128,

    /// delay between ping in seconds (float)
    #[argh(option, short = 'd', long = "delay", default = "get_default_delay()")]
    delay: f32,

    /// timeout in seconds when waiting for a ping to return (float)
    #[argh(option, short = 't', long = "timeout", default = "get_default_timeout()")]
    timeout: f32,

    /// timeout in seconds when resolving DNS (float)
    #[argh(option, short = 'l', long = "dns-timeout", default = "get_default_dns_timeout()")]
    dns_timeout: f32,

    /// STDOUT formatted to JSON
    #[argh(switch, short = 'j', long = "json")]
    print_json: bool,

    /// set to expect *all* pings to succeed
    #[argh(switch, short = 'e', long = "exclusive")]
    exclusive: bool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> ExitCode {
    let args: Args = argh::from_env();

    let (result_tx, result_rx) = broadcast::channel(10);
    let (cmd_tx, cmd_rx) = broadcast::channel(10);
    let lookup_task = 
        tokio::spawn(async move {
            run_lookup_task(
                args.host,
                result_tx,
                cmd_rx,
                Duration::from_secs_f32(args.dns_timeout))
                .await.expect("Lookup task failed")
        });

    let ping_task = 
        run_ping_task(result_rx,
            cmd_tx,
            args.print_json,
            Duration::from_secs_f32(args.timeout),
            Duration::from_secs_f32(args.delay),
            args.exclusive,
            args.count,
            args.resolve_cycles);

    let (ping_task_result, _) = join!(ping_task, lookup_task);

    match ping_task_result {
        Ok(host_up) => {
            if host_up {
                ExitCode::from(EXIT_CODE_HOST_UP)
            }
            else {
                ExitCode::from(EXIT_CODE_HOST_DOWN)
            }
        }
        Err(err) => {
            eprintln!("Ping task failed: {}", err.to_string());
            ExitCode::from(EXIT_CODE_ERROR)
        }
    }
    
}

async fn run_ping_task(
    mut ip_rx: broadcast::Receiver<LookupResult>,
    cmd_tx: broadcast::Sender<LookupTaskCommands>,
    print_json: bool,
    timeout: Duration,
    delay: Duration,
    exclusive: bool,
    count: Option<u128>,
    resolve_cycles: u128
) -> Result<bool, Error> {
    let mut sigint = signal(SignalKind::interrupt()).unwrap();

    let mut seq_count = 0u128;
    let mut host_is_up = false;
    let mut ipaddr = None;
    let pinger = tokio_icmp_echo::Pinger::new().await.unwrap();

    cmd_tx.send(LookupTaskCommands::Lookup)?;
    loop {
        if let Some(count) = count {
            if seq_count >= count {
                break;
            }
        }

        let delay_sleep = sleep(delay);
        tokio::pin!(delay_sleep);

        let mut to_print = None;
        tokio::select! {
            _ = &mut delay_sleep => {
                let seq_cnt = (seq_count % u16::MAX as u128) as u16;
                if let Some(ipaddr) = ipaddr {
                    if seq_count == 0 {
                        host_is_up = exclusive;
                    }
                    to_print = Some(
                        match pinger.ping(ipaddr, random(),seq_cnt, timeout).await {
                            Ok(ping_time) => {
                                if exclusive {
                                    host_is_up &= ping_time.is_some();
                                }
                                else {
                                    host_is_up |= ping_time.is_some();
                                }
                                generate_ping_success_msg(ping_time, ipaddr, seq_cnt, print_json)
                            },
                            Err(err) => generate_ping_error_msg(&err.to_string(), ipaddr, seq_cnt, print_json),
                        });
                    }

                    if seq_count > 0 && seq_count % resolve_cycles == 0 {
                        cmd_tx.send(LookupTaskCommands::Lookup)?;
                    }

                    seq_count += 1;
                }

                lookup_result = ip_rx.recv() => {
                    let lookup_result = lookup_result?;
                    to_print = Some(
                        match lookup_result {
                            LookupResult::Success(addr, hostname) => {
                                ipaddr = Some(addr);
                                generate_dns_success_msg(addr, hostname.as_str(), print_json)
                            },
                            LookupResult::Failure(err, hostname) => {
                                generate_dns_error_msg(&err.to_string(), hostname.as_str(), print_json)
                            }
                        }
                    );
                }

                _ = sigint.recv() => {
                    eprintln!("Got SIGINT; exiting");
                    cmd_tx.send(LookupTaskCommands::Exit)?;
                    break;
                }
            }

            if let Some(to_print_str) = to_print {
                println!("{}", to_print_str);
            }
        }

    Ok(host_is_up)
}

async fn run_lookup_task(
    hostname: String,
    result_tx: broadcast::Sender<LookupResult>,
    mut cmd_rx: broadcast::Receiver<LookupTaskCommands>,
    timeout: Duration
) -> Result<(), Error> {
    let hostname = Arc::new(hostname);
    loop {
        match &cmd_rx.recv().await? {
            LookupTaskCommands::Lookup => {
                match perform_lookup(hostname.as_str(), timeout).await {
                    Ok(ipaddr) => {
                        let _ = &result_tx.send(LookupResult::Success(ipaddr, Arc::clone(&hostname)))?;
                    },
                    Err(err) => {
                        let _ = &result_tx.send(LookupResult::Failure(Arc::new(err), Arc::clone(&hostname)))?;
                    },
                }
            },
            LookupTaskCommands::Exit => break,
        }
    }

    Ok(())
}

async fn perform_lookup(hostname: &str, timeout: Duration) -> Result<IpAddr, Error> {
    let timeout = sleep(timeout);
    tokio::pin!(timeout);

    let result;
    tokio::select! {
        _ = &mut timeout => {
            result = Err(anyhow!("Timeout on name lookup"));
        }

        lookup_result = lookup_host(format!("{}:0", hostname)) => {
            if let Ok(addrs) = lookup_result {
                let addrs: Vec<SocketAddr> = addrs.collect();
                result = Ok(addrs[0].ip());
            }
            else {
                result = Err(anyhow!(lookup_result.err().unwrap()));
            }
        }
    }

    result
}

fn generate_ping_success_msg(ping_time: Option<Duration>, ipaddr: IpAddr, seq_cnt: u16, as_json: bool) -> String {
    match ping_time {
        Some(ping_time) => {
            match as_json {
                true => {
                    let json_value = json!(
                        {
                            "type" : "ping",
                            "status" : "ok",
                            "address" : ipaddr.to_string(),
                            "duration" : ping_time.as_secs_f32(),
                            "unit" : "s"
                        }
                    );
                    json_value.to_string()
                }
                false => {
                    format!("Ping to {} responded in {} s (sequence_number={})",
                        ipaddr, ping_time.as_secs_f32(), seq_cnt)
                }
            }
        }
        None => {
            match as_json {
                true => {
                    let json_value = json!(
                        {
                            "type" : "ping",
                            "status" : "timeout",
                            "address" : ipaddr.to_string(),
                        }
                    );
                    json_value.to_string()
                },
                false => {
                    format!("Ping to {} timed out (sequence_number={})",
                        ipaddr, seq_cnt)
                }
            }
        }   
    }
}

fn generate_ping_error_msg(error_msg: &str, ipaddr: IpAddr, seq_cnt: u16, as_json: bool) -> String {
    match as_json {
        true => {
            let json_value = json!(
                {
                    "type" : "ping",
                    "status" : "error",
                    "reason" : error_msg,
                }
            );
            json_value.to_string()
        },
        false => {
            format!("Error sending ping to {} out (sequence_number={}): {}",
                ipaddr, seq_cnt, error_msg)
        }
    }
}

fn generate_dns_success_msg(ipaddr: IpAddr, hostname: &str, as_json: bool) -> String {
    match as_json {
        true => {
            let json_value = json!(
                {
                    "type" : "lookup",
                    "status" : "ok",
                    "hostname" : hostname,
                    "address"  : ipaddr.to_string()
                }
            );
            json_value.to_string()
        },
        false => {
            format!("{} resolved to {}", hostname, ipaddr)
        },
    }
}

fn generate_dns_error_msg(error_msg: &str, hostname: &str, as_json: bool) -> String {
    match as_json {
        true => {
            let json_value = json!(
                {
                    "type" : "lookup",
                    "status" : "error",
                    "hostname" : hostname,
                    "reason"  : error_msg
                }
            );
            json_value.to_string()
        },
        false => {
            format!("Failed to look up {}: {}", hostname, error_msg)
        },
    }
}

fn get_default_dns_renew() -> u128 {
    100
}

fn get_default_delay() -> f32 {
    1.0
}

fn get_default_timeout() -> f32 {
    5.0
}

fn get_default_dns_timeout() -> f32 {
    5.0
}
