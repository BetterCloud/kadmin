FROM bettercloudnonprod/projectx-dockerbaseimage:stable
MAINTAINER BetterCloud Engineers <engineering@bettercloud.com>

ADD build/libs/shared-kafka-admin-micro-0.9.0.war app.war
