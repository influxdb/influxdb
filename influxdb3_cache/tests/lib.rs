pub mod single_threaded;
pub mod multi_threaded;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;
