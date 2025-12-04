use opendal::raw::HttpFetch;

pub(crate) fn make_http_client() -> opendal::raw::HttpClient {
    opendal::raw::HttpClient::with(HttpClient::default())
}

#[derive(Debug, Default)]
struct HttpClient;

impl HttpFetch for HttpClient {
    async fn fetch(
        &self,
        req: http::Request<opendal::Buffer>,
    ) -> opendal::Result<http::Response<opendal::raw::HttpBody>> {
        unimplemented!()
    }
}
