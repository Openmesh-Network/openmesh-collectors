from l3_atom.stream_processing.standardisers import standardisers
import logging


def initialise_agents(app):
    """Initialises the Faust agents for each exchange raw topic"""
    for standardiser in standardisers:
        agent = standardiser()
        standardiser.raw_topic = app.topic(agent.raw_topic)
        logging.info(f"Initialising {agent.id} standardiser")
        app.agent(standardiser.raw_topic,
                  name=f"{agent.id}_agent")(agent.process)
        for k, v in agent.normalised_topics.items():
            agent.normalised_topics[k] = app.topic(v)
