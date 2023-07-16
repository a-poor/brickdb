use tonic::{Request, Response, Status};
use super::gen::{PingRequest, PingResponse};
use super::gen::database_server_server::{DatabaseServer, DatabaseServerServer};


pub fn create_service(server: BDBDatabaseServer) -> DatabaseServerServer<BDBDatabaseServer> {
    DatabaseServerServer::new(server)
}

#[derive(Debug, Default)]
pub struct BDBDatabaseServer;

impl BDBDatabaseServer {
    pub fn new() -> Self {
        BDBDatabaseServer {}
    }
}

#[tonic::async_trait]
impl DatabaseServer for BDBDatabaseServer {
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