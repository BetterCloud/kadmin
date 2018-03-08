FROM openjdk:8-jre
MAINTAINER Eimar Fandino

RUN mkdir /app
WORKDIR /app

ADD build/libs/shared-kafka-admin-micro-*.jar /app/app.jar
ADD application.properties /app/application.properties

EXPOSE 8080

ENTRYPOINT [ "java", "-jar", "/app/app.jar" , "--spring.profiles.active=kadmin,local"]