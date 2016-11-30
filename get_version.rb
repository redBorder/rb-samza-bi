#!/usr/bin/env ruby

require 'xmlsimple'

hash = XmlSimple.xml_in('pom.xml')
puts hash["version"]

