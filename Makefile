TARGETS=rmq_consume/rmq_consume rmq_publish/rmq_publish rmq_shovel/rmq_shovel
INSTALL=install -D -m 0755
PREFIX=/usr/local

all: $(TARGETS)

rmq_consume/rmq_consume: rmq_consume/src/* rmq_consume/rebar.config
	cd rmq_consume; make

rmq_publish/rmq_publish: rmq_publish/src/* rmq_publish/rebar.config
	cd rmq_publish; make

rmq_shovel/rmq_shovel: rmq_shovel/src/* rmq_shovel/rebar.config
	cd rmq_shovel; make

install: $(TARGETS)
	$(foreach target, $(TARGETS), $(INSTALL) $(target) $(PREFIX)/bin/;)

clean:
	cd rmq_consume; make clean
	cd rmq_publish; make clean
	cd rmq_shovel; make clean
