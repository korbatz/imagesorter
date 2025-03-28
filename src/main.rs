// External crate imports for enhanced functionality
use anyhow::{Context, Result};  // Error handling with context
use chrono::NaiveDateTime;      // Date/time handling without timezone
use clap::Parser;               // Command-line argument parsing
use crossbeam_channel::{bounded, Receiver};  // Multi-producer multi-consumer channels
use exif::{In, Tag};           // EXIF metadata extraction from images
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};  // Progress bars
use md5::{Digest, Md5};        // MD5 hashing for file comparison
use parking_lot::Mutex;         // Fast mutex implementation
use rayon::prelude::*;         // Parallel iteration and processing

// Standard library imports
use std::{
    collections::HashMap,       // Hash map for counting and tracking
    fs::{self, File},          // File system operations
    io::{BufReader, Read},     // Buffered I/O operations
    path::{Path, PathBuf},     // Path manipulation
    sync::Arc,                 // Thread-safe reference counting
    time::SystemTime,          // System time queries
};

// External crate for recursive directory walking
use walkdir::WalkDir;

/// Command line arguments structure
/// Uses clap's derive feature for automatic argument parsing
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input directory to process
    input_dir: String,

    /// Output directory for organized files
    output_dir: String,

    /// Minimum file size in bytes (supports suffixes KB, MB, GB)
    /// Uses clap's built-in argument parsing with default value
    #[arg(short, long, default_value = "64KB")]
    min_size: String,

    /// Number of worker threads (default: number of CPU cores)
    /// Option<usize> allows for None value when not specified
    #[arg(short, long)]
    threads: Option<usize>,

    /// Show thread usage statistics
    /// Boolean flag for enabling thread statistics output
    #[arg(short = 's', long)]
    show_thread_stats: bool,

    /// Copy files instead of moving them
    /// Boolean flag to control file operation mode
    #[arg(short, long)]
    copy: bool,
}

/// Statistics tracking structure
/// #[derive(Default)] automatically implements the Default trait
/// which provides default values for all fields
#[derive(Default)]
struct Stats {
    /// Counter for total files processed
    files_processed: usize,
    /// Counter for files successfully moved/copied
    files_moved: usize,
    /// Counter for files skipped due to size
    files_skipped_size: usize,
    /// Counter for duplicate files skipped
    files_skipped_dup: usize,
    /// Counter for files without creation date
    files_unknown_date: usize,
    /// Map tracking count of unknown file extensions
    unknown_extensions: HashMap<String, usize>,
    /// Counter for processing errors
    errors: usize,
    /// Map tracking files processed per thread
    thread_stats: HashMap<usize, usize>,
    /// Vector storing pairs of duplicate files
    duplicates: Vec<(PathBuf, PathBuf)>,
}

/// Main file processing structure
/// #[derive(Clone)] allows the structure to be cloned for thread safety
#[derive(Clone)]
struct ImageSorter {
    /// Thread-safe statistics using Arc (reference counting) and Mutex (synchronization)
    stats: Arc<Mutex<Stats>>,
    /// Minimum file size threshold in bytes
    min_size: u64,
    /// Progress bar manager for multiple progress bars
    progress_bars: MultiProgress,
    /// Flag indicating whether to copy or move files
    copy_mode: bool,
}

impl ImageSorter {
    /// Creates a new ImageSorter instance
    /// 
    /// # Arguments
    /// * `min_size` - Minimum file size in bytes to process
    /// * `copy_mode` - Whether to copy files instead of moving them
    /// 
    /// # Returns
    /// A new ImageSorter instance with default statistics and configured parameters
    fn new(min_size: u64, copy_mode: bool) -> Self {
        Self {
            // Arc provides thread-safe reference counting
            // Mutex ensures exclusive access to stats
            stats: Arc::new(Mutex::new(Stats::default())),
            min_size,
            // MultiProgress allows managing multiple progress bars
            progress_bars: MultiProgress::new(),
            copy_mode,
        }
    }

    fn get_file_checksum(&self, path: &Path) -> Result<String> {
        let mut file = File::open(path)?;
        let mut hasher = Md5::new();
        let mut buffer = [0; 8192]; // 8KB buffer for efficient reading

        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }

        Ok(format!("{:x}", hasher.finalize()))
    }

    fn get_creation_date(&self, path: &Path) -> Option<NaiveDateTime> {
        // Try EXIF data first
        if let Ok(file) = File::open(path) {
            let mut bufreader = BufReader::new(&file);
            if let Ok(exif) = exif::Reader::new().read_from_container(&mut bufreader) {
                // Try EXIF tags in order of preference:
                // 1. CreateDate - When the digital file was created
                // 2. DateTimeOriginal - When the photo was taken
                // 3. DateTime - General modification time
                // Try EXIF tags in order of preference:
                // 1. DateTimeOriginal - When the photo was taken (usually most accurate for photos)
                // 2. DateTime - General creation/modification time
                for date_tag in &[Tag::DateTimeOriginal, Tag::DateTime] {
                    if let Some(field) = exif.get_field(*date_tag, In::PRIMARY) {
                        if let Some(value) = field.value.display_as(*date_tag).to_string().strip_prefix("\'").and_then(|s| s.strip_suffix("\'")) {
                            if let Ok(dt) = NaiveDateTime::parse_from_str(&value, "%Y:%m:%d %H:%M:%S") {
                                return Some(dt);
                            }
                        }
                    }
                }
            }
        }

        // Fallback to filesystem metadata in order of preference
        if let Ok(metadata) = fs::metadata(path) {
            // Try creation time first
            if let Ok(created) = metadata.created() {
                if let Ok(duration) = created.duration_since(SystemTime::UNIX_EPOCH) {
                    if let Some(dt) = NaiveDateTime::from_timestamp_opt(duration.as_secs() as i64, 0) {
                        return Some(dt);
                    }
                }
            }
            
            // Then try modification time
            if let Ok(modified) = metadata.modified() {
                if let Ok(duration) = modified.duration_since(SystemTime::UNIX_EPOCH) {
                    if let Some(dt) = NaiveDateTime::from_timestamp_opt(duration.as_secs() as i64, 0) {
                        return Some(dt);
                    }
                }
            }
            
            // Last resort: current time
            let now = SystemTime::now();
            if let Ok(duration) = now.duration_since(SystemTime::UNIX_EPOCH) {
                if let Some(dt) = NaiveDateTime::from_timestamp_opt(duration.as_secs() as i64, 0) {
                    return Some(dt);
                }
            }
        }

        None
    }

    /// Process a single file, organizing it based on creation date
    /// 
    /// # Arguments
    /// * `path` - Path to the source file
    /// * `out_dir` - Base output directory
    /// * `pb` - Progress bar for this operation
    /// * `_thread_id` - Thread ID for statistics (unused)
    /// 
    /// # Returns
    /// * `Result<()>` - Success or error with context
    fn process_file(&self, path: &Path, out_dir: &Path, pb: &ProgressBar, _thread_id: usize) -> Result<()> {
        {
            let mut stats = self.stats.lock();
            stats.files_processed += 1;
        }

        let metadata = fs::metadata(path)?;
        if metadata.len() < self.min_size {
            let mut stats = self.stats.lock();
            stats.files_skipped_size += 1;
            return Ok(());
        }

        let target_dir = if let Some(date) = self.get_creation_date(path) {
            out_dir.join(date.format("%Y").to_string())
        } else {
            let mut stats = self.stats.lock();
            stats.files_unknown_date += 1;
            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("no_extension")
                .to_lowercase();
            *stats.unknown_extensions.entry(ext.clone()).or_default() += 1;
            out_dir.join("other").join(ext)
        };

        fs::create_dir_all(&target_dir)?;

        let file_name = path.file_name().unwrap();
        let mut target_path = target_dir.join(file_name);

        if target_path.exists() {
            let source_checksum = self.get_file_checksum(path)?;
            let target_checksum = self.get_file_checksum(&target_path)?;

            if source_checksum == target_checksum {
                let mut stats = self.stats.lock();
                stats.files_skipped_dup += 1;
                stats.duplicates.push((path.to_path_buf(), target_path.clone()));
                pb.println(format!("Duplicate found: {} -> {}", path.display(), target_path.display()));
                return Ok(());
            }

            // Generate unique name
            let stem = path.file_stem().unwrap().to_str().unwrap();
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            let mut counter = 1;
            while target_path.exists() {
                target_path = target_dir.join(format!("{}({}).{}", stem, counter, ext));
                counter += 1;
            }
        }

        if self.copy_mode {
            fs::copy(path, &target_path)?;
        } else {
            fs::rename(path, &target_path)?;
        }
        
        let mut stats = self.stats.lock();
        stats.files_moved += 1;
        pb.inc(1);
        
        Ok(())
    }

    /// Process files received through the channel
    /// 
    /// # Arguments
    /// * `rx` - Receiver end of the channel containing paths to process
    /// * `out_dir` - Base output directory
    /// 
    /// # Returns
    /// * `Result<()>` - Success or error with context
    /// 
    /// # Implementation Notes
    /// Uses a channel to receive work and processes files as they arrive
    fn process_files(&self, rx: Receiver<PathBuf>, out_dir: PathBuf) -> Result<()> {
        let thread_id = rayon::current_thread_index().unwrap_or(0);
        let pb = self.progress_bars.add(ProgressBar::new_spinner());
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} [{elapsed_precise}] {msg}")
                .unwrap(),
        );

        while let Ok(path) = rx.recv() {
            pb.set_message(format!("Processing: {}", path.display()));
            if let Err(e) = self.process_file(&path, &out_dir, &pb, thread_id) {
                let mut stats = self.stats.lock();
                stats.errors += 1;
                pb.println(format!("Error processing {}: {}", path.display(), e));
            }
        }

        pb.finish_with_message("Done");
        Ok(())
    }

    fn print_summary(&self, show_thread_stats: bool) {
        println!("\nProcessing Summary:");
        let stats = self.stats.lock();
        // Print duplicate files if any were found
        if stats.files_skipped_dup > 0 {
            println!("\nDuplicate files found:");
            for (src, dst) in &stats.duplicates {
                println!("  Source: {}", src.display());
                println!("  Already exists at: {}", dst.display());
                println!("");
            }
        }

        println!("\nStatistics:");
        println!("Total files processed: {}", stats.files_processed);
        println!("Files moved: {}", stats.files_moved);
        println!("Files skipped (too small): {}", stats.files_skipped_size);
        println!("Files skipped (duplicates): {}", stats.files_skipped_dup);
        println!("Files with unknown date: {}", stats.files_unknown_date);

        if !stats.unknown_extensions.is_empty() {
            println!("\nUnknown date files by extension:");
            let mut exts: Vec<_> = stats.unknown_extensions.iter().collect();
            exts.sort_by_key(|&(_, count)| std::cmp::Reverse(*count));
            for (ext, count) in exts {
                println!("  .{}: {} files", ext, count);
            }
        }

        println!("Errors encountered: {}", stats.errors);

        if show_thread_stats {
            println!("\nThread Statistics:");
            let mut thread_stats: Vec<_> = stats.thread_stats.iter().collect();
            thread_stats.sort_by_key(|&(id, _)| *id);
            
            let total_files = stats.files_processed as f64;
            for (&thread_id, &files) in thread_stats.iter() {
                let percentage = (files as f64 / total_files * 100.0) as u32;
                println!("  Thread {}: {} files ({}%)", thread_id, files, percentage);
            }
        }
    }
}

/// Parse a size string with optional suffix (KB, MB, GB) into bytes
/// 
/// # Arguments
/// * `size_str` - Size string (e.g., "64KB", "1MB", "1GB")
/// 
/// # Returns
/// * `Result<u64>` - Size in bytes or error if parsing fails
/// 
/// # Example
/// ```
/// let bytes = parse_size("64KB").unwrap();
/// assert_eq!(bytes, 65536);
/// ```
fn parse_size(size_str: &str) -> Result<u64> {
    let size_str = size_str.trim().to_uppercase();
    let multipliers = [
        ("KB", 1024),
        ("MB", 1024 * 1024),
        ("GB", 1024 * 1024 * 1024),
    ];

    for (suffix, multiplier) in multipliers {
        if size_str.ends_with(suffix) {
            let value = size_str[..size_str.len() - suffix.len()]
                .parse::<f64>()
                .context("Invalid size number")?;
            return Ok((value * multiplier as f64) as u64);
        }
    }

    size_str.parse::<u64>().context("Invalid size format")
}

/// Main entry point for the file organizer
/// 
/// # Returns
/// * `Result<()>` - Success or error with context
/// 
/// # Implementation Notes
/// 1. Parses command line arguments
/// 2. Sets up thread pool and channels
/// 3. Processes files in parallel
/// 4. Shows progress and statistics
fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    // Parse and initialize core components
    let min_size = parse_size(&args.min_size)?;
    let sorter = ImageSorter::new(min_size, args.copy);
    let out_dir = PathBuf::from(&args.output_dir);

    // Create a bounded channel for work distribution
    // The channel capacity of 1000 provides backpressure if processing is slower than discovery
    let (tx, rx) = bounded::<PathBuf>(1000);
    let out_dir_clone = out_dir.clone();

    // Set up parallel processing
    let num_threads = args.threads.unwrap_or_else(num_cpus::get);
    println!("Using {} worker threads", num_threads);

    // Set up progress tracking
    let pb = sorter.progress_bars.add(ProgressBar::new_spinner());
    
    // Create thread-safe references to shared state
    // Arc enables multiple threads to share ownership of the sorter
    let sorter_clone = Arc::new(sorter);
    let sorter_worker = Arc::clone(&sorter_clone);

    // Configure and create the thread pool using rayon
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)  // Use specified number of threads
        .build()?;

    // Spawn the worker thread pool
    pool.spawn(move || {
        // Convert channel receiver into parallel iterator
        // par_bridge() enables parallel processing of items from the channel
        rx.into_iter().par_bridge().for_each(|path| {
            // Get current thread's ID for statistics
            let thread_id = rayon::current_thread_index().unwrap_or(0);
            // Process each file in parallel
            if let Err(e) = sorter_worker.process_file(&path, &out_dir_clone, &ProgressBar::hidden(), thread_id) {
                eprintln!("Error processing {}: {}", path.display(), e);
            }
        });
    });
    // Configure progress bar with a spinner and elapsed time display
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {msg}")  // Green spinner with elapsed time
            .unwrap(),
    );

    // Recursively discover files in the input directory
    for entry in WalkDir::new(&args.input_dir)  // Start walking from input directory
        .follow_links(true)                     // Follow symbolic links during traversal
        .into_iter()                            // Convert to iterator
        .filter_map(|e| e.ok())                // Skip any entries that cause errors
        .filter(|e| e.file_type().is_file())   // Only process regular files
    {
        // Update progress bar with current file being discovered
        pb.set_message(format!("Found: {}", entry.path().display()));
        
        // Send file path to worker threads through the channel
        // to_path_buf() creates an owned PathBuf from a borrowed Path
        tx.send(entry.path().to_path_buf())?;
    }

    // Drop the sender to signal to receivers that no more files will be sent
    drop(tx);
    
    // Clean up progress bars
    sorter_clone.progress_bars.clear();
    
    // Print final statistics
    sorter_clone.print_summary(args.show_thread_stats);

    Ok(())
}
