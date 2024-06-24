use super::gen::internal_server_server::{InternalServer, InternalServerServer};
use super::gen::{PingRequest, PingResponse};
use tonic::{Request, Response, Status};

pub fn create_service(server: BDBInternalServer) -> InternalServerServer<BDBInternalServer> {
    InternalServerServer::new(server)
}

#[derive(Debug, Default)]
pub struct BDBInternalServer;

impl BDBInternalServer {
    pub fn new() -> Self {
        BDBInternalServer {}
    }
}

#[tonic::async_trait]
impl InternalServer for BDBInternalServer {
    async fn ping(
        &self,
        request: Request<PingRequest>, // Accept request of type HelloRequest
    ) -> Result<Response<PingResponse>, Status> {
        // Return an instance of type HelloReply
        println!("Got a request: {:?}", request);

        let reply = PingResponse {
            message: format!("Hello {}!", request.into_inner().name).into(), // We must use .into_inner() as the fields of gRPC requests and responses are private
        };

        Ok(Response::new(reply)) // Send back our formatted greeting
    }
}
