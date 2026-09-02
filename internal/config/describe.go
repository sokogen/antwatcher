package config

import "gopkg.in/yaml.v3"

// Describer decodes an opaque driver block into the driver's typed configuration.
// The returned value is rendered by Redacted, so Secret fields mask themselves,
// and when it implements DriverValidator it is checked by ValidateDrivers.
type Describer interface {
	Describe(raw yaml.Node) (any, error)
}

// DescriberFunc adapts a function to Describer.
type DescriberFunc func(raw yaml.Node) (any, error)

// Describe implements Describer.
func (f DescriberFunc) Describe(raw yaml.Node) (any, error) { return f(raw) }

// DriverValidator is implemented by typed driver configurations that must be
// checked against the core configuration, for example a broker driver enforcing
// `ack_wait > router.process_timeout + margin`.
type DriverValidator interface {
	ValidateWith(router Router) error
}

// Describers maps driver blocks to the Describer that understands them.
type Describers struct {
	// Bus is keyed by bus driver name.
	Bus map[string]Describer
	// Sinks is keyed by SinkKey(class, driver).
	Sinks map[string]Describer
}

// SinkKey builds the Describers.Sinks key for a class/driver pair.
func SinkKey(class, driver string) string { return class + "/" + driver }
