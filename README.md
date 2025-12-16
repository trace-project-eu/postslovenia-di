# Automatic Data Ingestor for Slovenian Pilot

This application serves as the **data ingestor for the Slovenian Pilot** and is designed to run as a standalone backend service. The service connects to PostSlovenia databases and stores the information in the TRACE DB.

##  Overview

This application serves as the **data injector** for the Slovenian Pilot. It is not designed to access directly, but interacts with the TRACE backend database to provides necessary data through a structured API.

##  Requirements

- **Python 3.12+**
- **httpx** - Python HTTP client for making synchronous and asynchronous requests with HTTP/2 support.
- **tenacity** - Library for flexible, configurable retry logic for handling transient failures in function calls and operations.


## Local testing
- **Build Docker Container** - `make build .` 
- **Verify the image** - `make images`
- **Run the container** - `make up`
- **To Stop** - `make down` 
