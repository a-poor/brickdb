use tonic::{transport::Server, Request, Response, Status};

use super::gen::internal_server_server::{InternalServer, InternalServerServer};
use super::gen::{PingRequest, PingResponse};


#[derive(Debug, Default)]
pub struct MyGreeter {}

#[tonic::async_trait]
impl InternalServer for MyGreeter {
    async fn ping(
        &self,
        request: Request<PingRequest>, // Accept request of type HelloRequest
    ) -> Result<Response<PingResponse>, Status> { // Return an instance of type HelloReply
        println!("Got a request: {:?}", request);

        let reply = PingResponse {
            message: format!("Hello {}!", request.into_inner().name).into(), // We must use .into_inner() as the fields of gRPC requests and responses are private
        };

        Ok(Response::new(reply)) // Send back our formatted greeting
    }
}
