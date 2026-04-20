package rangerdns

import (
	"fmt"
	"strconv"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	clog "github.com/coredns/coredns/plugin/pkg/log"
)

var pluginName = "ranger"

var log = clog.NewWithPlugin(pluginName)

// init registers this plugin.
func init() { plugin.Register(pluginName, setup) }

// setup is the function that gets called when the config parser see the token "example". Setup is responsible
// for parsing any extra options the example plugin may have. The first token this function sees is "example".
func setup(c *caddy.Controller) error {

	handler, err := parseAndCreate(c)
	if err != nil {
		return err
	}
	// Add the Plugin to CoreDNS, so Servers can use it in their plugin chain.
	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		handler.Next = next
		return handler

	})

	return nil
}

func parseAndCreate(c *caddy.Controller) (*RangerHandler, error) {
	c.Next() // Ignore "example" and give us the next token.
	config := NewRangerConfig()
	for c.NextBlock() {
		switch c.Val() {
		case "endpoint":
			args := c.RemainingArgs()
			if len(args) != 1 {
				return nil, c.ArgErr()
			}
			config.Endpoint = args[0]
		case "skip_data_from_replication_sources":
			args := c.RemainingArgs()
			if len(args) != 1 {
				return nil, c.ArgErr()
			}
			skipDataFromReplicationSourcesValue, err := strconv.ParseBool(args[0])
			if err != nil {
				return nil, err
			}
			config.SkipDataFromReplicationSources = skipDataFromReplicationSourcesValue
		default:
			return nil, fmt.Errorf("ranger: unknown argument %s found", c.Val())
		}
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	rangerClient, clientErr := NewRangerClient(config)
	if clientErr != nil {
		return nil, clientErr
	}

	rangerClient.Init()
	return NewRangerHandler(&rangerClient), nil
}
