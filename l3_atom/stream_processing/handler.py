from l3_atom.stream_processing.sources import standardisers

def initialise_agents(app):
    for standardiser in standardisers:
        agent = standardiser()
        app.agent(standardiser.raw_topic)(agent.process)