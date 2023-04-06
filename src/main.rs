use std::{
    collections::{HashMap, VecDeque},
    time::Instant,
    sync::{Arc, Mutex},
    iter,
    thread,
};

use task::{Task, TaskType};

use crossbeam::deque::{Injector, Stealer, Steal, Worker};

fn main() {
    let (seed, starting_height, max_children) = get_args();

    eprintln!(
        "Using seed {}, starting height {}, max. children {}",
        seed, starting_height, max_children
    );

    let count_map = Arc::new(Mutex::new(HashMap::new()));
    let taskq = Task::generate_initial(seed, starting_height, max_children);
    let output = Arc::new(Mutex::new(0_u64));

    let max_thread = num_cpus::get_physical();
    let mut threads = Vec::new();

    let injector = Arc::new(Injector::new());
    for task in taskq {
        injector.push(task);
    }

    let mut workers = VecDeque::with_capacity(max_thread);
    let mut stealers = Vec::with_capacity(max_thread);
    for _ in 0..max_thread {
        let worker = Worker::new_lifo();
        stealers.push(worker.stealer());
        workers.push_back(worker);
    }
    let stealers = Arc::new(stealers);

    let start = Instant::now();
    for _ in 0..max_thread {

        let output = output.clone();
        let count_map = count_map.clone();
        let injector = injector.clone();
        let stealers = stealers.clone();
        let worker = workers.pop_front().unwrap();
    
        let thread = thread::spawn(move || {
            while let Some(task) = find_task(&worker, &injector, &stealers) {
                {
                    *(count_map.lock().unwrap()).entry(task.typ).or_insert(0usize) += 1;
                }
                let result = task.execute();
                {
                    let mut lock = output.lock().unwrap();
                    *lock ^= result.0;
                }
                for task in result.1 {
                    worker.push(task);
                }
            }
        });
        threads.push(thread);
    }

    // main thread do not require these anymore
    drop(injector);
    drop(workers);
    drop(stealers);

    for thread in threads {
        _ = thread.join();
    }

    let end = Instant::now();

    eprintln!("Completed in {} s", (end - start).as_secs_f64());

    let lock = output.lock().unwrap();
    let count = count_map.lock().unwrap();

    println!(
        "{},{},{},{}",
        *lock,
        *count.get(&TaskType::Hash).unwrap_or(&0),
        *count.get(&TaskType::Derive).unwrap_or(&0),
        *count.get(&TaskType::Random).unwrap_or(&0)
    );
}


fn find_task(
    worker: &Worker<Task>,
    injector: &Injector<Task>,
    stealers: &[Stealer<Task>],
) -> Option<Task> {
    worker.pop().or_else(|| {
        // Otherwise, we need to look for a task elsewhere.
        iter::repeat_with(|| {
            // Or try stealing a task from one of the other threads.
            stealers.iter().map(|s| s.steal()).collect::<Steal<Task>>()
                // Try stealing a batch of tasks from the global queue.
                .or_else(|| injector.steal())

        })
        // Loop while no task was stolen and any steal operation needs to be retried.
        .find(|s| !s.is_retry())
        // Extract the stolen task, if there is one.
        .and_then(|s| s.success())
    })
}

// There should be no need to modify anything below

fn get_args() -> (u64, usize, usize) {
    let mut args = std::env::args().skip(1);
    (
        args.next()
            .map(|a| a.parse().expect("invalid u64 for seed"))
            .unwrap_or_else(|| rand::Rng::gen(&mut rand::thread_rng())),
        args.next()
            .map(|a| a.parse().expect("invalid usize for starting_height"))
            .unwrap_or(5),
        args.next()
            .map(|a| a.parse().expect("invalid u64 for seed"))
            .unwrap_or(5),
    )
}

mod task;
