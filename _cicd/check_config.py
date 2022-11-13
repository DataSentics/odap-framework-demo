import yaml


def check_destinations_exist(config):
    for i in config["exports"].keys():
        if not config["exports"][i]["destination"] in config["destinations"].keys():
            raise Exception


with open("config.yaml", "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)["parameters"]["segmentfactory"]


check_destinations_exist(config)
