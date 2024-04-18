PANDA_DB

Configuration files:
* panda_db/panda.cnf - copied into container during build stage

Loading data:
* panda_db/entrypoint/create_user.sql - runs during container startup
* panda_db/entrypoint/panda.sql - a sqldump file, runs during container startup

Scripts:
* panda_db/scripts/do_dump.sh - sqldump wrapping script, used to take db snapshots  

Taking a snapshot:

~~~shell
# from a remote host, piping through gzip
$ ssh alex@p1.foxpropanda.com "docker exec panda_db ./do_dump.sh - | gzip" | gunzip > dump.sql
~~~

Building an image:

~~~shell
# panda_db at the end is the location of Dockerfile
$ panda_db: docker build -t panda_db:latest panda_db
~~~

Strting a container:

~~~shell
# need to explicitly specify port-forwarding
# -v (volumes) need proper local and container paths 
$ docker run --rm -d --name panda_db -p 3306:3306 -v /host/path:/container/path registry.gitlab.com/yelluas/panda/panda_db:test
~~~

