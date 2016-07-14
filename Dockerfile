FROM bettercloudnonprod/projectx-dockerbaseimage:stable
MAINTAINER BetterCloud Engineers <engineering@bettercloud.com>

ADD build/libs/kadmin-micro-0.1.0.war app.war
