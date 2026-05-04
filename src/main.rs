use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const TOTAL_TASKS: usize = 1000;
const WORKER_COUNT: usize = 8;
const TASK_DURATION_MS: u64 = 200;
const ARRIVAL_INTERVAL_MS: u64 = 20;
const CPU_TASK_COST: u32 = 35;
const IO_TASK_COST: u32 = 10;
const CPU_LIMIT: u32 = 100;
const SEED: u64 = 3340;


struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        SimpleRng { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        // Simple linear congruential generator for reproducible simulated workloads.
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.state
    }

    fn gen_range(&mut self, upper: u32) -> u32 {
        (self.next_u64() % upper as u64) as u32
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TaskKind {
    IO,
    CPU,
}

impl TaskKind {
    fn cpu_cost(&self) -> u32 {
        match self {
            TaskKind::IO => IO_TASK_COST,
            TaskKind::CPU => CPU_TASK_COST,
        }
    }
}

#[derive(Clone, Debug)]
struct Task {
    id: usize,
    kind: TaskKind,
    arrival_time: Instant,
    duration_ms: u64,
}

#[derive(Clone, Copy, Debug)]
enum Policy {
    Fifo,
    Optimized,
}

impl Policy {
    fn name(&self) -> &'static str {
        match self {
            Policy::Fifo => "FIFO",
            Policy::Optimized => "Optimized",
        }
    }
}

#[derive(Debug)]
struct CompletedTask {
    worker_id: usize,
    task_id: usize,
    kind: TaskKind,
    wait_ms: u128,
    turnaround_ms: u128,
}

#[derive(Clone, Debug)]
struct MonitorSample {
    cpu_usage: u32,
    active_workers: usize,
}

#[derive(Debug)]
struct SimulationResult {
    policy: String,
    io_percent: u32,
    cpu_percent: u32,
    total_tasks: usize,
    completed_tasks: usize,
    cpu_completed: usize,
    io_completed: usize,
    total_runtime_ms: u128,
    average_cpu_usage: f64,
    average_worker_usage: f64,
    average_wait_ms: f64,
    average_turnaround_ms: f64,
    max_wait_ms: u128,
}

fn main() {
    let results = vec![
        run_simulation(Policy::Fifo, 70),
        run_simulation(Policy::Optimized, 70),
        run_simulation(Policy::Fifo, 80),
        run_simulation(Policy::Optimized, 80),
    ];

    fs::create_dir_all("experiment_outputs").expect("could not create experiment_outputs folder");

    for result in &results {
        let file_name = format!(
            "experiment_outputs/{}_{}_io.txt",
            result.policy.to_lowercase(),
            result.io_percent
        );
        let mut file = File::create(&file_name).expect("could not create output file");
        write!(file, "{}", format_result(result)).expect("could not write output file");
    }

    println!("\n================ FINAL COMPARISON ================");
    for result in &results {
        println!(
            "{} {}% IO / {}% CPU -> runtime: {} ms, avg CPU: {:.2}%, avg workers: {:.2}/8",
            result.policy,
            result.io_percent,
            result.cpu_percent,
            result.total_runtime_ms,
            result.average_cpu_usage,
            result.average_worker_usage
        );
    }
    println!("Experiment output files were saved in experiment_outputs/.");
}

fn run_simulation(policy: Policy, io_percent: u32) -> SimulationResult {
    println!("\n================ {} | {}% IO / {}% CPU ================", policy.name(), io_percent, 100 - io_percent);

    let start_time = Instant::now();
    let current_cpu = Arc::new(Mutex::new(0u32));
    let active_workers = Arc::new(Mutex::new(0usize));
    let stop_monitor = Arc::new(AtomicBool::new(false));
    let samples: Arc<Mutex<Vec<MonitorSample>>> = Arc::new(Mutex::new(Vec::new()));

    let monitor_cpu = Arc::clone(&current_cpu);
    let monitor_workers = Arc::clone(&active_workers);
    let monitor_stop = Arc::clone(&stop_monitor);
    let monitor_samples = Arc::clone(&samples);

    let monitor_handle = thread::spawn(move || {
        while !monitor_stop.load(Ordering::SeqCst) {
            let cpu = *monitor_cpu.lock().unwrap();
            let workers = *monitor_workers.lock().unwrap();
            monitor_samples.lock().unwrap().push(MonitorSample {
                cpu_usage: cpu,
                active_workers: workers,
            });
            thread::sleep(Duration::from_millis(10));
        }
    });

    let (task_tx, task_rx) = mpsc::channel::<Task>();
    let (available_tx, available_rx) = mpsc::channel::<usize>();
    let (completed_tx, completed_rx) = mpsc::channel::<CompletedTask>();

    let mut worker_senders = Vec::new();
    let mut worker_handles = Vec::new();

    for worker_id in 0..WORKER_COUNT {
        let (worker_tx, worker_rx) = mpsc::channel::<Option<Task>>();
        worker_senders.push(worker_tx);

        let available_tx_clone = available_tx.clone();
        let completed_tx_clone = completed_tx.clone();
        let worker_cpu = Arc::clone(&current_cpu);
        let worker_active = Arc::clone(&active_workers);

        let handle = thread::spawn(move || {
            available_tx_clone.send(worker_id).unwrap();

            while let Ok(message) = worker_rx.recv() {
                match message {
                    Some(task) => {
                        let started_at = Instant::now();
                        {
                            let mut active = worker_active.lock().unwrap();
                            *active += 1;
                        }

                        thread::sleep(Duration::from_millis(task.duration_ms));

                        {
                            let mut cpu = worker_cpu.lock().unwrap();
                            *cpu = cpu.saturating_sub(task.kind.cpu_cost());
                        }
                        {
                            let mut active = worker_active.lock().unwrap();
                            *active = active.saturating_sub(1);
                        }

                        let completed_at = Instant::now();
                        let completed = CompletedTask {
                            worker_id,
                            task_id: task.id,
                            kind: task.kind,
                            wait_ms: started_at.duration_since(task.arrival_time).as_millis(),
                            turnaround_ms: completed_at.duration_since(task.arrival_time).as_millis(),
                        };

                        completed_tx_clone.send(completed).unwrap();
                        available_tx_clone.send(worker_id).unwrap();
                    }
                    None => break,
                }
            }
        });
        worker_handles.push(handle);
    }

    drop(available_tx);
    drop(completed_tx);

    let manager_cpu = Arc::clone(&current_cpu);
    let manager_handle = thread::spawn(move || {
        manager_loop(policy, task_rx, available_rx, completed_rx, worker_senders, manager_cpu)
    });

    // Main thread acts as the producer. It creates a fixed 1000 tasks at 20ms intervals.
    let mut rng = SimpleRng::new(SEED + io_percent as u64);
    for id in 0..TOTAL_TASKS {
        let roll = rng.gen_range(100);
        let kind = if roll < io_percent { TaskKind::IO } else { TaskKind::CPU };
        let task = Task {
            id,
            kind,
            arrival_time: Instant::now(),
            duration_ms: TASK_DURATION_MS,
        };
        task_tx.send(task).expect("manager stopped receiving tasks");
        thread::sleep(Duration::from_millis(ARRIVAL_INTERVAL_MS));
    }
    drop(task_tx);

    let manager_result = manager_handle.join().unwrap();

    stop_monitor.store(true, Ordering::SeqCst);
    monitor_handle.join().unwrap();

    for handle in worker_handles {
        handle.join().unwrap();
    }

    let runtime_ms = start_time.elapsed().as_millis();
    let sample_data = samples.lock().unwrap();
    let average_cpu = if sample_data.is_empty() {
        0.0
    } else {
        sample_data.iter().map(|s| s.cpu_usage as f64).sum::<f64>() / sample_data.len() as f64
    };
    let average_workers = if sample_data.is_empty() {
        0.0
    } else {
        sample_data.iter().map(|s| s.active_workers as f64).sum::<f64>() / sample_data.len() as f64
    };

    let result = SimulationResult {
        policy: policy.name().to_string(),
        io_percent,
        cpu_percent: 100 - io_percent,
        total_tasks: TOTAL_TASKS,
        completed_tasks: manager_result.completed_tasks,
        cpu_completed: manager_result.cpu_completed,
        io_completed: manager_result.io_completed,
        total_runtime_ms: runtime_ms,
        average_cpu_usage: average_cpu,
        average_worker_usage: average_workers,
        average_wait_ms: manager_result.average_wait_ms,
        average_turnaround_ms: manager_result.average_turnaround_ms,
        max_wait_ms: manager_result.max_wait_ms,
    };

    println!("{}", format_result(&result));
    result
}

#[derive(Debug)]
struct ManagerResult {
    completed_tasks: usize,
    cpu_completed: usize,
    io_completed: usize,
    average_wait_ms: f64,
    average_turnaround_ms: f64,
    max_wait_ms: u128,
}

fn manager_loop(
    policy: Policy,
    task_rx: mpsc::Receiver<Task>,
    available_rx: mpsc::Receiver<usize>,
    completed_rx: mpsc::Receiver<CompletedTask>,
    worker_senders: Vec<mpsc::Sender<Option<Task>>>,
    manager_cpu: Arc<Mutex<u32>>,
) -> ManagerResult {
    let mut all_queue: VecDeque<Task> = VecDeque::new();
    let mut io_queue: VecDeque<Task> = VecDeque::new();
    let mut cpu_queue: VecDeque<Task> = VecDeque::new();
    let mut available_workers: VecDeque<usize> = VecDeque::new();

    let mut generator_done = false;
    let mut completed_tasks = 0usize;
    let mut cpu_completed = 0usize;
    let mut io_completed = 0usize;
    let mut total_wait_ms = 0u128;
    let mut total_turnaround_ms = 0u128;
    let mut max_wait_ms = 0u128;


    loop {
        while let Ok(worker_id) = available_rx.try_recv() {
            available_workers.push_back(worker_id);
        }

        loop {
            match task_rx.try_recv() {
                Ok(task) => match policy {
                    Policy::Fifo => all_queue.push_back(task),
                    Policy::Optimized => match task.kind {
                        TaskKind::IO => io_queue.push_back(task),
                        TaskKind::CPU => cpu_queue.push_back(task),
                    },
                },
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    generator_done = true;
                    break;
                }
            }
        }

        while let Ok(done) = completed_rx.try_recv() {
            completed_tasks += 1;
            match done.kind {
                TaskKind::CPU => cpu_completed += 1,
                TaskKind::IO => io_completed += 1,
            }
            total_wait_ms += done.wait_ms;
            total_turnaround_ms += done.turnaround_ms;
            max_wait_ms = max_wait_ms.max(done.wait_ms);
            // The worker releases the reserved CPU after the task completes.
        }

        dispatch_ready_tasks(policy, &mut all_queue, &mut io_queue, &mut cpu_queue, &mut available_workers, &worker_senders, &manager_cpu);

        if generator_done && completed_tasks == TOTAL_TASKS {
            break;
        }

        thread::sleep(Duration::from_millis(1));
    }

    for sender in worker_senders {
        let _ = sender.send(None);
    }

    ManagerResult {
        completed_tasks,
        cpu_completed,
        io_completed,
        average_wait_ms: total_wait_ms as f64 / completed_tasks as f64,
        average_turnaround_ms: total_turnaround_ms as f64 / completed_tasks as f64,
        max_wait_ms,
    }
}

fn dispatch_ready_tasks(
    policy: Policy,
    all_queue: &mut VecDeque<Task>,
    io_queue: &mut VecDeque<Task>,
    cpu_queue: &mut VecDeque<Task>,
    available_workers: &mut VecDeque<usize>,
    worker_senders: &[mpsc::Sender<Option<Task>>],
    manager_cpu: &Arc<Mutex<u32>>,
) {
    while !available_workers.is_empty() {
        let task_option = match policy {
            Policy::Fifo => choose_fifo_task(all_queue, manager_cpu),
            Policy::Optimized => choose_optimized_task(io_queue, cpu_queue, manager_cpu),
        };

        let task = match task_option {
            Some(task) => task,
            None => break,
        };

        let worker_id = available_workers.pop_front().unwrap();
        {
            let mut cpu = manager_cpu.lock().unwrap();
            *cpu += task.kind.cpu_cost();
        }
        worker_senders[worker_id].send(Some(task)).unwrap();
    }
}

fn choose_fifo_task(queue: &mut VecDeque<Task>, manager_cpu: &Arc<Mutex<u32>>) -> Option<Task> {
    let current_cpu = *manager_cpu.lock().unwrap();
    if let Some(front) = queue.front() {
        if current_cpu + front.kind.cpu_cost() <= CPU_LIMIT {
            return queue.pop_front();
        }
    }
    None
}

fn choose_optimized_task(
    io_queue: &mut VecDeque<Task>,
    cpu_queue: &mut VecDeque<Task>,
    manager_cpu: &Arc<Mutex<u32>>,
) -> Option<Task> {
    let current_cpu = *manager_cpu.lock().unwrap();

    // Optimized idea:
    // - Use CPU tasks only when there is enough CPU budget.
    // - When CPU usage is high, fill the remaining lanes with IO tasks.
    // - This naturally creates useful batches like 2 CPU + 3 IO, 1 CPU + 6 IO, or 0 CPU + 8 IO.
    if !cpu_queue.is_empty() && current_cpu + CPU_TASK_COST <= CPU_LIMIT {
        // If CPU is still low, schedule a CPU task so CPU-heavy work does not starve.
        if current_cpu <= 60 || io_queue.is_empty() {
            return cpu_queue.pop_front();
        }
    }

    if !io_queue.is_empty() && current_cpu + IO_TASK_COST <= CPU_LIMIT {
        return io_queue.pop_front();
    }

    if !cpu_queue.is_empty() && current_cpu + CPU_TASK_COST <= CPU_LIMIT {
        return cpu_queue.pop_front();
    }

    None
}

fn format_result(result: &SimulationResult) -> String {
    format!(
        "Policy: {}\nTask distribution: {}% IO / {}% CPU\nTotal tasks generated: {}\nTotal tasks completed: {}\nIO tasks completed: {}\nCPU tasks completed: {}\nWorker pool size: {}\nTask duration: {} ms\nArrival interval: {} ms\nCPU rule: IO = {}%, CPU = {}%, global CPU <= {}%\nTotal runtime: {} ms\nAverage CPU usage: {:.2}%\nAverage active workers: {:.2}/{}\nAverage wait time: {:.2} ms\nAverage turnaround time: {:.2} ms\nMax wait time: {} ms\n",
        result.policy,
        result.io_percent,
        result.cpu_percent,
        result.total_tasks,
        result.completed_tasks,
        result.io_completed,
        result.cpu_completed,
        WORKER_COUNT,
        TASK_DURATION_MS,
        ARRIVAL_INTERVAL_MS,
        IO_TASK_COST,
        CPU_TASK_COST,
        CPU_LIMIT,
        result.total_runtime_ms,
        result.average_cpu_usage,
        result.average_worker_usage,
        WORKER_COUNT,
        result.average_wait_ms,
        result.average_turnaround_ms,
        result.max_wait_ms
    )
}
