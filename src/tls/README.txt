= Transport Layer Security Management =

This directory contains code for the management of TLS configuration
and use.

Note: The terms "Connector" and "Listener" are used in this document
as generic names for protocol specific connector and listener
records. For example the term "Connector" implies both AMQP connectors
(record type "connector") as well as TCP connectors (record type
"tcpConnector").

== Configuration ==

The sslProfile management entity specifies a set of certificates,
keys, and password information that is used to instantiate one or more
TLS configuration instances. sslProfiles may be created/read/updated
and deleted via the router's management agent.

Listener and Connector management entities will include the name of a
particular sslProfile when TLS is required on connections associated
with the Listener/Connector. TLS parameters that are specific to a
Listener/Connector are also included (e.g. authenticate peer or
validate peer hostname).

See the router management schema for more details.

== Primary Data Structures ==

There are five primary data structures used for TLS support:

- qd_tls_context_t (private)
- qd_ssl2_profile_t
- qd_tls_config_t
- qd_tls_session_t
- qd_proton_config_t

The root structure is the qd_tls_context_t. There is a one-to-one
relationship between a qd_tls_context_t and an sslProfile management
entity instance. In other words each sslProfile that is configured has
a qd_tls_context_t instance representing it.

The qd_ssl2_profile_t structure contains the values from a given
sslProfile entity. A qd_tls_context_t has an embedded
qd_ssl2_profile_t instance for holding the current values from the
sslProfile record.

A qd_tls_config_t represents an instance of an Openssl configuration
context. A qd_tls_config_t is created and owned by the
Listener/Connector which is configured to use an sslProfile. In order
to support management updates to an sslProfile record all
qd_tls_config_ts are held in a list in the qd_tls_context_t associated
with the qd_tls_config_t's parent sslProfile. The qd_tls_config_t
creates a qd_proton_config_t using the sslProfile and configuration
settings from its parent Listener/Connector.

A qd_proton_config_t is created by a qd_tls_config_t and wraps the
underlying Proton SSL/TLS configuration instance. This is either an
instance of a pn_ssl_domain_t (for AMQP) or a pn_tls_config_t (for raw
connection). This wrapper fufills two requirements: 1) creation and
deletion of Proton TLS sessions using the same configuration MUST be
single threaded, and 2) the Proton configuration MUST NOT be freed
until all sessions using the configuration have been freed. The
qd_proton_config_t contains a mutex and an atomic reference count to
satisfy these requirements. qd_proton_config_ts are immutable. In
order to update a configuration a new qd_proton_config_t is created to
replace the old.

A qd_tls_session_t is the per-connection TLS state. It is created
using the qd_tls_config_t and qd_proton_config_t associated with the
connection's parent Listener/Connector.

=== Object Lifecycle ===

A qd_tls_context_t is created when a new sslProfile record is created
by the management agent. It is destroyed after the management agent
has deleted the corresponding sslProfile record and all child
qd_tls_config_t instances have been freed.

A qd_tls_config_t instance is reference-counted. The
Listener/Connector that created the qd_tls_config_t holds a reference
to it. A reference is also held by the parent qd_tls_context_t. A
qd_tls_config_t instance is created when a Listener/Connector is
instantiated via the management agent. It is destroyed after the
parent Listener/Connector is deleted.

A qd_proton_config_t is reference counted by the parent
qd_tls_config_t and all qd_tls_session_ts using it.

A qd_tls_session_t is created when a new protocol connection is
opened. It is released when the associated connection is terminated.

=== Object Relationship Diagram ===

    +--- qd_tls_context_t ----+
    |                         |
    | +- qd_ssl2_profile_t -+ |
    | |     "Profile1"      | |
    | +---------------------+ |
    |                         |
    +---+---------------------+
        |
        |
        |   +- qd_tls_config_t-+      +- qd_tls_config_t -+
        +-->|                  | <--> |                   | <--> ...
            +-----------------++      +-------------------+
              ^               |           ^
              |               |           |
              |               |           |
            +-+- Listener -+  |         +-+- Listener -+
            |   "Foo"      |  |         |   "Bar"      |
            +--------------+  |         +--------------+
                              |
                              |
                              +------------------------+
                                                       |
                                                       V
                     +- qd_tls_session_t -+         +- qd_proton_config_t -+
                     |     "Conn1"        |         |                      |
                     |                    +-------->+                      |
                     |                    |         |                      |
                     +--------------------+         +--+-------------------+
                                                       ^
                                                       |
                     +- qd_tls_session_t -+            |
                     |     "Conn2"        |            |
                     |                    +------------+
                     |                    |
                     +--------------------+

In the above diagram there are two Listener instances both sharing the
sslProfile record "Profile1".  Each listener maintains its own
qd_tls_config_t. The qd_tls_config_t's are held in a list on the
parent qd_tls_context_t. Listener "Foo" has two active TLS
connections. These connections have a dedicated qd_tls_session_t
instance. The sessions reference the qd_proton_config_t used to create
them.


== Threading

The management operations - create/update/delete - for sslProfile,
Listener, and Connector management entities occur on the management
agent thread. Therefore these operations are not multithreaded. This
allows the implementation to avoid using locks to manage
qd_tls_context_t and qd_tls_config_t/qd_proton_config_t
instances. Debug code is present to assert that these operations are
only called via the management agent thread.

Per-connection qd_tls_session_t operations may occur on any I/O thread
and therefore can run simultaineously with the management agent
thread. This means sslProfile updates (and deletions) may occur while
I/O threads are using the qd_tls_sesson_t that are dependent on the
sslProfile configuration. There are mutexes in the qd_tls_config_t and
the qd_proton_config_t that prevent races between the I/O threads and
management. See below.

== sslProfile Management Update

When an sslProfile is created or updated it is necessary to read the
associated certficate files. Accessing the filesystem takes an
indeterminate amount of time during which the calling thread is
blocked. Therefore it is important not to do this operation on an I/O
thread as it blocks other I/O work for a potentially long time
(hundreds of milliseconds). Doing so would result in long latency
times.

This implementation avoids this problem by loading the certificates
during the create/update operation which occurs on the management
thread. Therefore the management thread assumes the cost of the
operation and I/O threads are not impacted.

However updating an sslProfile requires all child qd_tls_config_t
instances update their run-time configuration. This means that all
qd_tls_config_ts have to re-load their certificate files and generate
new qd_proton_config_t instances. While this is being done (on the
management thread) new qd_tls_session_t instances using that
qd_tls_config_t and its current qd_proton_config_t may be created by
the I/O threads (e.g. a new connection arrives).

Two mutexes are used to prevent race conditions between the management
thread and the I/O threads.

First there is a mutex in the qd_tls_config_t that protects its
pointer to the qd_proton_config_t instance. Keep in mind that a
qd_proton_config_t is immutable and must be replaced in order to
update the TLS configuration. When an sslProfile management operation
occurs this lock is held long enough to swap out the old
qd_proton_config_t instance with a new qd_proton_config_t instance
containing the updated management configuration. This same lock must
be held by an I/O thread when the qd_proton_config_t pointer is copied
into a new qd_tls_session_t instance and the qd_proton_config_t's
reference count is incremented. This prevents the qd_proton_config_t
from being deleted during the qd_tls_session_t creation process.

Second there is a mutex in the qd_proton_config_t that must be held
when a Proton session is created or destroyed. This is necessary because
these operations are not thread safe.

Since qd_proton_config_t are reference counted and immutable there is
no need to nest these locks.

== Files

- tls.c: main codebase for sslProfile management, TLS configuration
  and session lifecycle logic.
- tls_amqp.c: those parts of the API that are specific to using TLS
  with Proton AMQP connections.
- tls_raw.c: those parts of the API that are specific to use the
  buffer-based TLS implementation. These APIs are used by the Proton
  Raw connection transports.

