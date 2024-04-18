FROM mariadb:10.4.3-bionic
LABEL maintainer Alex Yelluas <ayelluas@gmail.com>

# Better terminal support
ENV TERM=screen-256color
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# db will pick up time zone from the system
ENV TZ=America/Los_Angeles
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /koalaty

ENV MYSQL_ROOT_PASSWORD=koalaty
ENV MYSQL_DATABASE=koalaty

# extra maraidb config values
COPY ./koalaty.cnf /etc/mysql/conf.d/

# db dump script
COPY ./scripts/do_dump.sh /koalaty
RUN chmod 755 /koalaty/do_dump.sh

COPY ./entrypoint/create_user.sql /docker-entrypoint-initdb.d/

# panda db dump file
# COPY ./entrypoint/panda.sql /docker-entrypoint-initdb.d/
