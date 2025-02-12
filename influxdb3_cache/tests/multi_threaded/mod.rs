use futures::future::join_all;

fn produce_data(kib: usize) -> Vec<u8> {
    let chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        .chars()
        .map(|c| c as u8)
        .cycle();
    let data: Vec<u8> = chars.take(1024 * kib).collect();
    data
}

async fn ingester() {


}


async fn querier() {

}


#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn run_perf_test() {
    let handle_1 = tokio::spawn(async move {
        ingester().await;
    });

    let handle_2 = tokio::spawn(async move {
        querier().await;
    });

    join_all([handle_1, handle_2]).await;
}
