pub mod decrease_replication;
pub mod increase_replication;
pub mod merge_partition;
pub mod remove_partition;
pub mod split_partition;

pub fn circular_get<T>(vec: &Vec<T>, idx: usize) -> Option<&T> {
    if vec.is_empty() {
        return None;
    }
    let idx = idx % vec.len();
    return vec.get(idx);
}
