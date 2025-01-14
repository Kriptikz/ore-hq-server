use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sysinfo::{CpuRefreshKind, Disks, MemoryRefreshKind, RefreshKind, System};
use tokio::{sync::mpsc::UnboundedReceiver, time::Instant};

use crate::app_metrics::{AppMetrics, AppMetricsEvent};

pub async fn metrics_system(
    url: String,
    token: String,
    org: String,
    bucket: String,
    host: String,
    mut metrics_event: UnboundedReceiver<AppMetricsEvent>
) {
    let app_metrics = AppMetrics::new(url, token, org, bucket, host);
    let mut sys = System::new();
    let mut disks = Disks::new_with_refreshed_list();

    let r_kind = RefreshKind::new()
        .with_cpu(CpuRefreshKind::everything().without_frequency())
        .with_memory(MemoryRefreshKind::everything());

    let mut system_stats_instant = Instant::now();

    loop {
        tick_system_stats_metrics(&app_metrics, &mut sys, &r_kind, &mut system_stats_instant, &mut disks).await;
        while let Ok(me) = metrics_event.try_recv() {
            match me {
                AppMetricsEvent::MineEvent(data) => {
                    match data {
                        crate::app_metrics::AppMetricsMineEvent::V1(data) => {
                            let ts_ns = match SystemTime::now().duration_since(UNIX_EPOCH) {
                                Ok(d) => {
                                    d.as_nanos()
                                },
                                Err(_d) => {
                                    tracing::error!(target: "server_log", "Time went backwards...");
                                    continue;
                                }
                            };
                            let formatted_data = format!("mine_event,host={} balance={}u,difficulty={}u,last_hash_at={}i,timing={}i,reward={}u,boost_1={}u,boost_2={}u,boost_3={}u {}",
                                app_metrics.hostname,
                                data.balance,
                                data.difficulty,
                                data.last_hash_at,
                                data.timing,
                                data.reward,
                                data.boost_1,
                                data.boost_2,
                                data.boost_3,
                                ts_ns
                            );
                            match app_metrics.send_data_to_influxdb(formatted_data).await {
                                Ok(_) => {
                                },
                                Err(e) => {
                                    tracing::error!(target: "server_log", "Failed to send metrics data to influxdb.\nError: {:?}", e);
                                }
                            }

                        },
                        crate::app_metrics::AppMetricsMineEvent::V2(data) => {
                            let ts_ns = match SystemTime::now().duration_since(UNIX_EPOCH) {
                                Ok(d) => {
                                    d.as_nanos()
                                },
                                Err(_d) => {
                                    tracing::error!(target: "server_log", "Time went backwards...");
                                    continue;
                                }
                            };
                            let formatted_data = format!("mine_event,host={} balance={}u,difficulty={}u,last_hash_at={}i,timing={}i,reward={}u,boost_1={}u,boost_2={}u,boost_3={}u {}",
                                app_metrics.hostname,
                                data.balance,
                                data.difficulty,
                                data.last_hash_at,
                                data.timing,
                                data.net_reward,
                                0.0,
                                0.0,
                                0.0,
                                ts_ns
                            );
                            match app_metrics.send_data_to_influxdb(formatted_data).await {
                                Ok(_) => {
                                },
                                Err(e) => {
                                    tracing::error!(target: "server_log", "Failed to send metrics data to influxdb.\nError: {:?}", e);
                                }
                            }

                        }

                    }
                },
                AppMetricsEvent::ClaimEvent(_data) => {
                },
                AppMetricsEvent::ProcessingClaimsEvent(data) => {
                    let ts_ns = match SystemTime::now().duration_since(UNIX_EPOCH) {
                        Ok(d) => {
                            d.as_nanos()
                        },
                        Err(_d) => {
                            tracing::error!(target: "server_log", "Time went backwards...");
                            continue;
                        }
                    };
                    let formatted_data = format!("claim_system_event,host={} queue_length={}u {}",
                        app_metrics.hostname,
                        data.claims_queue_length,
                        ts_ns
                    );
                    match app_metrics.send_data_to_influxdb(formatted_data).await {
                        Ok(_) => {
                        },
                        Err(e) => {
                            tracing::error!(target: "server_log", "Failed to send metrics data to influxdb.\nError: {:?}", e);
                        }
                    }

                },
                AppMetricsEvent::RouteEvent(data) => {
                    let ts_ns = match SystemTime::now().duration_since(UNIX_EPOCH) {
                        Ok(d) => {
                            d.as_nanos()
                        },
                        Err(_d) => {
                            tracing::error!(target: "server_log", "Time went backwards...");
                            continue;
                        }
                    };
                    let formatted_data = format!("route_event,host={} route=\"{}\",method=\"{}\",status_code={}u,request={}u,response={}u,latency={}u {}",
                        app_metrics.hostname,
                        data.route,
                        data.method,
                        data.status_code,
                        data.request,
                        data.response,
                        data.latency,
                        ts_ns
                    );
                    match app_metrics.send_data_to_influxdb(formatted_data).await {
                        Ok(_) => {},
                        Err(e) => {
                            tracing::error!(target: "server_log", "Failed to send metrics data to influxdb.\nError: {:?}", e);
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn tick_system_stats_metrics(app_metrics: &AppMetrics, sys: &mut System, r_kind: &RefreshKind, system_stats_instant: &mut Instant, disks: &mut Disks) {
    if system_stats_instant.elapsed().as_secs() >= 5 {
        // track metrics
        sys.refresh_specifics(*r_kind);
        disks.refresh_list();
        let ts_ns = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => {
                d.as_nanos()
            },
            Err(_d) => {
                tracing::error!(target: "server_log", "Time went backwards...");
                return;
            }
        };
        let mut cpu_data = String::new();
        let mut total_cpu_usage = 0.;

        for (i, cpu) in sys.cpus().iter().enumerate() {
            let formatted_data = format!("cpu,host={},cpu={}i usage={} {}\n",
                app_metrics.hostname,
                i,
                cpu.cpu_usage(),
                ts_ns,
            );
            total_cpu_usage += cpu.cpu_usage();
            cpu_data.push_str(&formatted_data);
        }

        let cpu_total_usage_data = format!("cpu,host={} total_usage={} {}\n",
            app_metrics.hostname,
            total_cpu_usage,
            ts_ns,
        );

        cpu_data.push_str(&cpu_total_usage_data);


        let mut disk_data = String::new();
        let mut total_disk_used = 0;
        let mut total_disk_total = 0;
        for (i, disk) in disks.iter().enumerate() {
            let available_space = disk.available_space();
            let total_space = disk.total_space();
            let used_space = total_space - available_space;
            let formatted_data = format!("disk,host={},disk={}i used={},total={} {}\n",
                app_metrics.hostname,
                i,
                used_space,
                disk.total_space(),
                ts_ns,
            );
            total_disk_used += used_space;
            total_disk_total += total_space;
            disk_data.push_str(&formatted_data);
        }
        let disk_total_usage_data = format!("disk,host={},disk=all used={},total={} {}\n",
            app_metrics.hostname,
            total_disk_used,
            total_disk_total,
            ts_ns,
        );

        disk_data.push_str(&disk_total_usage_data);

        let memory_data = format!(
            "memory,host={} total={}i,used={}i,free={}i {}",
            app_metrics.hostname,
            sys.total_memory(),
            sys.used_memory(),
            sys.free_memory(),
            ts_ns,
        );

        let metrics_data = format!("{}\n{}\n{}\n",
            cpu_data,
            disk_data,
            memory_data,
        );
        match app_metrics.send_data_to_influxdb(metrics_data).await {
            Ok(_) => {
            },
            Err(e) => {
                tracing::error!(target: "server_log", "Failed to send metrics data to influxdb.\nError: {:?}", e);
            }
        }
        // reset instant
        *system_stats_instant = Instant::now();
    }
}
