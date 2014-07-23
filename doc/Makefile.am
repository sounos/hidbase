#automake 
EXTRA_DIST = rc.hitrackerd.ini rc.hitrackerd.html rc.hitrackerd base64.php \
rc.hichunkd.ini rc.hichunkd.html rc.hichunkd  hitrackerd.init hichunkd.init hidbase.spec

install: install-ini

conftodir = "`echo $(sysconfdir)|sed -e 's|/usr/etc|/etc|'`"

initdir = "$(DESTDIR)$(conftodir)/init.d"
rcdir = "$(DESTDIR)$(conftodir)/rc.d/init.d"

sysconftodir = "$(DESTDIR)$(conftodir)"

htmldir = "`echo @prefix@"/var/html"|sed -e 's|/usr/var|/var|g'`"
htmltodir = "$(DESTDIR)$(htmldir)"
isubuntu = "`uname -v|grep Ubuntu`"

install-ini:
	@echo install *.html to $(htmldir)
	@test -d $(htmltodir)/hichunk || mkdir -p $(htmltodir)/hichunk
	@cp -p rc.hichunkd.html $(htmltodir)/hichunk/index.html
	@test -d $(htmltodir)/hitracker || mkdir -p $(htmltodir)/hitracker
	@cp -p rc.hitrackerd.html $(htmltodir)/hitracker/index.html
	@echo install hitrackerd.ini hichunkd.ini to $(sysconftodir)
	@test -d $(sysconftodir) || mkdir -p $(sysconftodir)
	@test -f $(sysconftodir)/hitrackerd.ini \
		|| cp -p rc.hitrackerd.ini $(sysconftodir)/hitrackerd.ini
	@test -f $(sysconftodir)/hichunkd.ini \
		|| cp -p rc.hichunkd.ini $(sysconftodir)/hichunkd.ini
	@perl -i -p -e "s|\"/var/html|\"$(htmldir)|g" \
		$(sysconftodir)/hichunkd.ini \
		$(sysconftodir)/hitrackerd.ini 
	@echo install hitrackerd.init  hichunkd.init to $(initdir)
	@test -d $(initdir) || mkdir -p $(initdir)
	@test -f $(initdir)/functions  || ln -sf $(rcdir)/functions $(initdir)/functions
	@if [ $(isubuntu) != "" ]; \
	then \
		@test -f $(initdir)/hitrackerd \
			|| cp -p hitrackerd.init $(initdir)/hitrackerd; \
		@test -f $(initdir)/hichunkd \
			|| cp -p hichunkd.init $(initdir)/hichunkd; \
	else \
		@test -f $(initdir)/hitrackerd \
			|| cp -p rc.hitrackerd $(initdir)/hitrackerd; \
		@test -f $(initdir)/hichunkd \
			|| cp -p rc.hichunkd $(initdir)/hichunkd; \
	fi 
	@perl -i -p -e "s@/usr/local/sbin@$(prefix)/sbin@g" $(initdir)/hitrackerd 
	@perl -i -p -e "s@/usr/local/etc@$(conftodir)@g" $(initdir)/hitrackerd 
	@perl -i -p -e "s@/usr/local/sbin@$(prefix)/sbin@g" $(initdir)/hichunkd 
	@perl -i -p -e "s@/usr/local/etc@$(conftodir)@g" $(initdir)/hichunkd 
