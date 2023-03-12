# Logging

Skupper Router produces two kinds of logs. First, the operation logs and then flow logs.

This document deals with the operation logs only.

## OpenShift

[OpenShift 4.12 by default uses Fluentd, Elasticsearch, and Kibana](https://access.redhat.com/documentation/en-us/openshift_container_platform/4.12/html/logging/cluster-logging) for its logging subsystem.
[Fluentd collects the logs](https://docs.openshift.com/container-platform/4.12/logging/config/cluster-logging-collector.html), then
[Elasticsearch is used to store the collected data](https://docs.openshift.com/container-platform/4.12/logging/config/cluster-logging-log-store.html), and finally there is
[Kibana to visualize the collected data](https://docs.openshift.com/container-platform/4.12/logging/cluster-logging-visualizer.html).

In the above, Fluentd can be [replaced by Vector](https://access.redhat.com/documentation/en-us/openshift_container_platform/4.12/html/logging/cluster-logging#cluster-logging-about-vector_cluster-logging), and
[an external log store can substitute for Logstash](https://access.redhat.com/documentation/en-us/openshift_container_platform/4.12/html/logging/cluster-logging#cluster-logging-forwarding-about_cluster-logging).   

### Log parsing

At some point throughout the log processing pipeline, the logs, which skrouterd produces in a plain text format, need to be parsed.
Every component of the pipeline is capable of performing this step
Fluentd [<parse> directive](https://docs.fluentd.org/configuration/parse-section)
Vector [remap with the VRL language](https://vector.dev/docs/reference/vrl/) or transform with an [arbitrary lua program](https://vector.dev/docs/reference/configuration/transforms/lua/)
Elasticsearch [Grok filter plugin](https://www.elastic.co/guide/en/logstash/current/plugins-filters-grok.html)

AFAIK (TODO) OpenShift wants me to use grok in elasticsearch, as I haven't found where to configure this in fluentd or vector, but I might be wrong.

* https://stackoverflow.com/questions/57460451/how-to-extract-and-visualize-values-from-a-log-entry-in-openshift-efk-stack

#### Date and time

    ruby -e 'require "time"; puts Time.strptime("2023-03-12 11:54:24.084418 +0100", "%Y-%m-%d %H:%M:%S.%N %z")'

In general, the log processing pipeline may consists from gathering the log messages, 

### Log collection

#### Fluentd

```shell
sudo dnf install -y ruby-devel
gem install fluentd --no-doc
```

Check the config syntax with `fluentd --dry-run -c fluent.conf`

Test with

    fluentd -c fluentd.conf

#### Vektor

```shell
sudo dnf install -y https://packages.timber.io/vector/0.28.1/vector-0.28.1-1.$(arch).rpm
```

Test with

    vector -c vector.conf < sample.log

#### Elasticsearch

From https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html

```shell
# this then requires a root user to run elasticsearch
sudo dnf install -y https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.6.2-x86_64.rpm

# this allows running as regular user
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.6.2-linux-x86_64.tar.gz
tar -xzf elasticsearch-8.6.2-linux-x86_64.tar.gz

# probably best to use docker anyways
docker network create elastic
docker run --name elasticsearch --net elastic --rm -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -it docker.elastic.co/elasticsearch/elasticsearch:8.6.2
docker run --name kibana --net elastic --rm -p 5601:5601 -it docker.elastic.co/kibana/kibana:8.6.2
```

The install will print superuser password, username is `elastic` (GUnJ9xh-RfxjMEC4v6BG)

From https://www.elastic.co/guide/en/welcome-to-elastic/current/getting-started-general-purpose.html
and https://www.elastic.co/guide/en/elasticsearch/reference/current/run-elasticsearch-locally.html

Test with

    ES_PATH_CONF=elasticsearch.config.yaml elasticsearch

Upload logs with

    curl -X POST 127.0.0.1:9200/skrouterd/_logs/1 -T sample.log

Open Kibana dev console () and run Grok query 

### Visualization

## Kibana

### Putting it all together





[]{}