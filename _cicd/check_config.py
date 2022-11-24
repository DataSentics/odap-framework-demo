import yaml


def check_destinations_exist(config):
    for i in config["exports"].keys():
        if not config["exports"][i]["destination"] in config["destinations"].keys():
            raise RuntimeError("Destination definition is not present")


try:
    with open("config.yaml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)["parameters"]["segmentfactory"]
except:
    raise RuntimeError("Unable to load config")

check_destinations_exist(config)
