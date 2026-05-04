# Concurrent Task Dispatcher in Rust

## Project title
Concurrent Task Dispatcher in Rust

## How to build and run

```bash
cargo build
cargo run
```

The program runs four experiment cases automatically:

```text
FIFO, 70% IO / 30% CPU
Optimized, 70% IO / 30% CPU
FIFO, 80% IO / 20% CPU
Optimized, 80% IO / 20% CPU
```

Each run uses:

- fixed 1000 tasks
- 20ms arrival interval
- 8 worker threads
- IO task = 200ms sleep and 10% CPU resource
- CPU task = 200ms sleep and 35% CPU resource
- global CPU limit = 100%
- monitor samples every 10ms

After running, output files are saved in:

```text
experiment_outputs/
```

## Design summary

This project simulates a small operating-system-style task dispatcher. The main thread creates tasks and sends them into a manager queue. The manager thread receives tasks, stores them in queues, checks CPU availability, checks worker availability, and sends work to a bounded pool of 8 worker threads. A separate monitor thread records CPU usage and worker usage every 10ms.

The design uses Rust channels for message passing between the producer, manager, and workers. It uses `Arc<Mutex<_>>` for shared CPU usage and active worker count because those values need to be read by multiple threads safely.

## Task model

Each task has:

- id
- task kind: IO or CPU
- arrival time
- duration

IO tasks use 10% CPU resource. CPU tasks use 35% CPU resource. The dispatcher never allows global CPU usage to pass 100%.

## Scheduling policies

### FIFO

The FIFO policy uses one queue. The manager tries to dispatch the front task first. If the front task cannot fit under the CPU limit, it waits. This is simple but can cause head-of-line blocking.

### Optimized

The optimized policy separates IO and CPU tasks into different queues. The manager tries to keep the system busy without exceeding the 100% CPU limit. It naturally creates useful worker mixes such as:

- 2 CPU + 3 IO = 100% CPU
- 1 CPU + 6 IO = 95% CPU
- 0 CPU + 8 IO = 80% CPU

This improves runtime because IO tasks can still run while CPU-heavy tasks are waiting for CPU capacity.

## Metrics collected

The program prints:

- total tasks completed
- total runtime
- average CPU usage
- average active workers
- average wait time
- average turnaround time
- max wait time
- number of CPU tasks completed
- number of IO tasks completed

## Experiment summary

The main comparison is FIFO vs optimized. FIFO is easier to understand, but it can waste worker lanes when a CPU-heavy task at the front of the queue cannot run. The optimized scheduler improves runtime by allowing IO tasks to run when CPU capacity is limited.

## Tool Use Disclosure

I used ChatGPT to help organize the project structure, explain Rust concurrency choices, and check that the project matched the final project requirements.

One piece of advice I accepted was to use channels for passing tasks between the main thread, manager, and workers because this matches the producer-consumer design.

One piece of advice I rejected/fixed was an earlier design that only used a simple shared queue and did not track global CPU usage. That version was not enough because the final project required a 100% CPU resource limit and a monitor thread.
