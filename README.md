# l3_data_collection
A suite of programs for data collection and enhancement. Currently, data is retrieved and normalised from a variety of different crypto exchanges, including both centralised and decentralised sources.

## Background
This project is an MVP for GDA Fund's L3 Atom Open Data Initiative. It is a collection of data from multiple exchanges, and is intended to be used as a base for a future framework for L3 Atom. For ease of future modification, each exchange is segmented into distinct folders and the logic/implementation is contained therein. Data is collected and normalised into standardised table formats, as outlined in GDA's [medium article](https://gdafund.medium.com/open-crypto-data-initiative-1e096ccbf0e6) for L3 Atom.

Below is a description of how you can recreate our process of setting up the MVP, but if you want to connect to the server we're personally running, see our [documentation](https://www.notion.so/gda-fund/L3-Atom-MVP-Websocket-1f1b691eb7824a948e8680cdb552c8e0). Everything after this point is for developers who are curious in our process -- connecting to the MVP yourself is as simple as connecting to a websocket.

## Install

All required dependencies can be installed from `requirements.txt`.
```
pip install -r requirements.txt
```
The easiest way to get the data collection suite working on your local machine is to use [docker and docker-compose](https://docs.docker.com/desktop/windows/install/).
## Usage
Clone the repository and modify the code to connect to your own running Kafka cluster. In Kafka, add topics `<exchange>-raw`, `<exchange>-normalised` and `<exchange>-trades` for every exchange supported in the repo. The collectors will start producing data to all three topics for all exchanges. A file for setting up the Kafka cluster locally and automatically adding the required topics is coming soon.

Put any required key and environment files into the `./keys` directory and they will automatically be mounted onto the containers. 
### Running with Docker Compose
Navigate to the main directory (with `docker-compose.yml`) and run 
```
docker-compose up -d
``` 
to start collecting. 

To stop collecting, run 
```
docker-compose down
```
### Python
If you don't have docker and docker-compose installed, run 
```
python src/<exchange>.py
``` 
to start collecting data for each individual exchange and push it into your Kafka cluster.
