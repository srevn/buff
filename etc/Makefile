# buff packaging — install the binary and the host-OS service template

MAKEFLAGS += --no-print-directory

PREFIX  ?= /usr/local
BINDIR  ?= $(PREFIX)/bin

BUILD_OS ?= $(shell uname -s | tr '[:upper:]' '[:lower:]')
BUILT_BIN ?= ../bin/buff

BUFF_USER  ?= $(shell id -un "$${SUDO_USER:-$$(id -u)}")
BUFF_GROUP ?= $(shell id -gn $(BUFF_USER))

# Per-host destinations and the values substituted into the template
ifeq ($(BUILD_OS),linux)
    SYSCONFDIR  ?= /etc/buff
    SYSTEMD_DIR ?= /etc/systemd/system
endif
ifeq ($(BUILD_OS),freebsd)
    SYSCONFDIR ?= $(PREFIX)/etc/buff
    RCD_DIR    ?= $(PREFIX)/etc/rc.d
endif
ifeq ($(BUILD_OS),darwin)
    SYSCONFDIR     ?= $(PREFIX)/etc/buff
    LAUNCHD_DIR    ?= $(HOME)/Library/LaunchAgents
    LAUNCHD_DOMAIN ?= gui/$(shell id -u)
    BUFF_DATA_DIR  ?= $(HOME)/.local/share/buff
    BUFF_LOG       ?= $(HOME)/Library/Logs/buff.log
endif
# Fallback so the variable is always defined on an unrecognised host
SYSCONFDIR ?= $(PREFIX)/etc/buff

INSTALL         ?= install
INSTALL_PROGRAM ?= $(INSTALL) -m 0755
INSTALL_DATA    ?= $(INSTALL) -m 0644

# Rewrite the @TOKEN@ placeholders to resolved install paths
SUBST = sed -e 's|@BUFF_BIN@|$(BINDIR)/buff|g' \
            -e 's|@BUFF_CONF@|$(SYSCONFDIR)/buff.env|g' \
            -e 's|@BUFF_DATA_DIR@|$(BUFF_DATA_DIR)|g' \
            -e 's|@BUFF_LOG@|$(BUFF_LOG)|g' \
            -e 's|@BUFF_USER@|$(BUFF_USER)|g' \
            -e 's|@BUFF_GROUP@|$(BUFF_GROUP)|g'

.PHONY: install install-server install-client install-bin uninstall

# bare `install` stays the full server install — the documented deployment path.
install: install-server

install-bin:
	@test -e "$(BUILT_BIN)" || $(MAKE) -s -C .. dist
	@$(INSTALL) -d $(DESTDIR)$(BINDIR)
	@$(INSTALL_PROGRAM) $(BUILT_BIN) $(DESTDIR)$(BINDIR)/buff
	@echo "Installed $(BINDIR)/buff"

install-client: install-bin ## install just the buff binary (client; talks to a remote server)
	@echo "Client install. Set BUFF_URL or --server to your server, or bake a default: make dist SERVER_URL=https://relay."

install-server: install-bin ## install the binary + buff.env + host-OS service template
	@$(INSTALL) -d $(DESTDIR)$(SYSCONFDIR)
	@if [ ! -e $(DESTDIR)$(SYSCONFDIR)/buff.env ]; then \
	    $(INSTALL_DATA) buff.env.example $(DESTDIR)$(SYSCONFDIR)/buff.env; \
	    echo "Installed $(SYSCONFDIR)/buff.env"; \
	else \
	    echo "Preserved existing $(SYSCONFDIR)/buff.env"; \
	fi
	@case "$(BUILD_OS)" in \
	  linux) \
	    $(INSTALL) -d $(DESTDIR)$(SYSTEMD_DIR); \
	    $(SUBST) systemd/buff.service > $(DESTDIR)$(SYSTEMD_DIR)/buff.service; \
	    chmod 0644 $(DESTDIR)$(SYSTEMD_DIR)/buff.service; \
	    echo "Installed $(SYSTEMD_DIR)/buff.service (runs as $(BUFF_USER):$(BUFF_GROUP))"; \
	    echo "Next: systemctl daemon-reload && systemctl enable --now buff" ;; \
	  darwin) \
	    $(INSTALL) -d $(DESTDIR)$(LAUNCHD_DIR) $(DESTDIR)$(BUFF_DATA_DIR); \
	    $(SUBST) launchd/io.buff.plist > $(DESTDIR)$(LAUNCHD_DIR)/io.buff.plist; \
	    chmod 0644 $(DESTDIR)$(LAUNCHD_DIR)/io.buff.plist; \
	    echo "Installed $(LAUNCHD_DIR)/io.buff.plist"; \
	    echo "Next: launchctl bootstrap $(LAUNCHD_DOMAIN) $(LAUNCHD_DIR)/io.buff.plist" ;; \
	  freebsd) \
	    $(INSTALL) -d $(DESTDIR)$(RCD_DIR); \
	    $(SUBST) freebsd/buff > $(DESTDIR)$(RCD_DIR)/buff; \
	    chmod 0755 $(DESTDIR)$(RCD_DIR)/buff; \
	    echo "Installed $(RCD_DIR)/buff (runs as $(BUFF_USER):$(BUFF_GROUP))"; \
	    echo "Next: sysrc buff_enable=YES buff_data_dir=/var/db/buff && service buff start" ;; \
	  *) \
	    echo "buff: no service template for host '$(BUILD_OS)' — binary + env installed, service skipped" ;; \
	esac

uninstall: ## remove the binary + host-OS service template
	@rm -f $(DESTDIR)$(BINDIR)/buff
	@echo "Removed $(BINDIR)/buff"
	@case "$(BUILD_OS)" in \
	  linux) \
	    if [ -z "$(DESTDIR)" ]; then systemctl disable --now buff 2>/dev/null || true; fi; \
	    rm -f $(DESTDIR)$(SYSTEMD_DIR)/buff.service; \
	    if [ -z "$(DESTDIR)" ]; then systemctl daemon-reload 2>/dev/null || true; fi; \
	    echo "Removed $(SYSTEMD_DIR)/buff.service" ;; \
	  darwin) \
	    if [ -z "$(DESTDIR)" ]; then launchctl bootout $(LAUNCHD_DOMAIN) $(LAUNCHD_DIR)/io.buff.plist 2>/dev/null || true; fi; \
	    rm -f $(DESTDIR)$(LAUNCHD_DIR)/io.buff.plist; \
	    echo "Removed $(LAUNCHD_DIR)/io.buff.plist" ;; \
	  freebsd) \
	    if [ -z "$(DESTDIR)" ]; then service buff onestop 2>/dev/null || true; fi; \
	    rm -f $(DESTDIR)$(RCD_DIR)/buff; \
	    echo "Removed $(RCD_DIR)/buff" ;; \
	esac
	@echo "Left $(SYSCONFDIR)/buff.env and the data directory in place"
