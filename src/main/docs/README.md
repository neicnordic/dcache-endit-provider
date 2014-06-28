Channel handler plugin for xrootd4j and dCache
==============================================

This is channel handler plugin for xrootd4j and dCache.

Using the plugin with the standalone server
-------------------------------------------

To use the plugin with the standalone server of xrootd4j, download the
standalone server from http://github.com/gbehrmann/xrootd4j and start
it with something like:

    java -Dlog=debug 
         -jar /path/to/xrootd4j/xrootd4j-standalone-1.3.1-jar-with-dependencies.jar \
         --plugins /path/to/plugins/ \
         --handler authn:none,redirect

where /path/to/plugins is the directory containing the directory
containing this file.


Using the plugin with dCache
----------------------------

To use this plugin with dCache, place the directory containing this
file in /usr/local/share/dcache/plugins/

To enable the plugin, define the following property in dcache.conf:

    xrootdPlugins=auth:none,redirect
