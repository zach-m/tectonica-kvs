tectonica-kvs
=

Simple, yet powerful, framework (and approach) for handling a key-value store. It allows for multiple concurrent readers but a single
concurrent writer to access each entry. It also provides a read-before-update mechanism which makes life simpler and keeps data consistent.
Indexing and caching are also supported as part of the framework. The interface is intuitive and straightforward. This framework provides a
base class, and a set of subclasses implementing it on several backend persistence engines, including in-memory (which is great for development).

The usage of this framework would probably yield the most benefit when used to prototype a data model, where changes are frequent and not
backwards-compatible. However, in more than a few scenarios, this framework can be used in production. The heavy-lifting is done by the
backend database and cache either way.

To use in your project, add the following to your `pom.xml`:

	<dependency>
		<groupId>com.tectonica</groupId>
		<artifactId>tectonica-kvs</artifactId>
		<version>0.6.1</version>
	</dependency>

 