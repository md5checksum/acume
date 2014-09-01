#!/usr/bin/python

import os, sys, re, xml, commands
import xml.dom.minidom as DOM

from xml.dom.minidom import parse

replace_list = ['IntelligentCache']

output = commands.getstatusoutput('find . -name "pom.xml"')[1]
file_list = output.split('\n')

super_version = None


if len(sys.argv) > 1:
   super_version = sys.argv[1]
else:
   super_version = '${super.version}'

def updateVersion(root):
   artifactId = None
   for child in [child for child  in root.childNodes if child.nodeType != DOM.Element.TEXT_NODE and child.nodeType != DOM.Element.COMMENT_NODE]:
      if child.tagName == 'artifactId':
         artifactId = child.firstChild.data
      elif child.tagName == 'version' and artifactId in replace_list:
         child.firstChild.data = super_version
      elif child.tagName in ['parent', 'dependencies', 'dependency', 'properties']:
         updateVersion(child)
         
   for filename  in file_list:
      print 'working on file: ' + filename
      dom = parse(filename)
      root = dom.getElementsByTagName("project")[0]
      updateVersion(root)
      root.writexml(open(filename, 'w'))
      
      

