#!/usr/bin/env python

# DIRAC file catalogue client,
# metadata and file link ingestion
#
# V5: sql database on eiscathq
# V6: Default to logfile dirac-metadata-pop-yyyymmddTHHMMSS.log
# V7: sql database via eth1 10.0.0.x
# V8: running in Docker on LXC container dirac.eiscat.se
# V9: corr start and end time
#
# Latest modified by C-F Enell 2020
"""
Eiscat file catalogue and metadata population for DIRAC

Usage:
eiscat-catalog-bulk-population-script.py -y YYYY [-u] [-p]

Requirements:
- Access to SQL database disk_archive
- Access to EISCAT L2 file archive

This work is co-funded by the EOSC-hub project (Horizon 2020) under
Grant number 777536.
"""

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC import S_OK, S_ERROR

listyear=None
updatemeta = False
pbar=False
registerChecksum=False

def setlistyear( arg ):
  global listyear
  if listyear or not arg:
    Script.showHelp()
    return S_ERROR()
  listyear = arg
  return S_OK()

def setupdatemeta( arg ):
  global updatemeta
  updatemeta = True
  print "Updating metadata of existing catalogue entries"
  return S_OK()

def setpbar( arg ):
  global pbar
  pbar = True
  return S_OK()

def setRegisterChecksum(arg):
  global registerChecksum
  registerChecksum = True
  return S_OK()

# Define a help message
Script.setUsageMessage( """
Eiscat file catalog client to work in the top of EISCAT filesystem

Usage:
   %s [option]
""" % Script.scriptName )

# Register accepted switches and their callbacks
Script.registerSwitch( "y:", "listyear=", "Year folder to process (mandatory)", setlistyear )
Script.registerSwitch( "u", "updatemeta", "If entry in catalogue, update metadata information (optional)", setupdatemeta )
Script.registerSwitch( "p", "progressbar", "Show progress bar (optional)", setpbar)
Script.registerSwitch( "C", "checksum", "Register file checksum", setRegisterChecksum)

Script.parseCommandLine( ignoreErrors = False )


# Check that year was given
if listyear == None:
  Script.showHelp()
  DIRAC.exit( -1 )
else:
  listyear="/"+listyear+"/"

fcType = 'FileCatalog'

import sys, os
import datetime
import uuid
from DIRAC.Resources.Catalog.FileCatalogFactory import FileCatalogFactory
from DIRAC.Core.Utilities.Adler import fileAdler

#class Capturing(list):
#    def __enter__(self):
#        self._stdout = sys.stdout
#        sys.stdout = self._stringio = StringIO()
#        return self
#    def __exit__(self, *args):
#        self.extend(self._stringio.getvalue().splitlines())
#        sys.stdout = self._stdout



############################
# Redirect output to logfile
############################
dt=datetime.datetime.utcnow()
logfile='/home/dirac/dirac-metadata-pop-%4.4d%2.2d%2.2dT%2.2d%2.2d%2.2d.log' % (dt.year,dt.month,dt.day,dt.hour,dt.minute,dt.second)
lgf=open(logfile,'w')
#sys.stdout=lgf

print "listyear folder to process"
print listyear

########################################
# IMPORTANT global root paths:
# example pfn: /archive/2007/tau8v_zenith_1.11_EI@vhf/20071102_09/26387999.mat.bz2
# example lfn: /eiscat.se/archive/2007/tau8v_zenith_1.11_EI@vhf/20071102_09/26387999.mat.bz2
########################################
root_lfn='/eiscat.se/test' #DIRAC
root_pfn='/mnt/archive' #Bind mount in docker

#################################################
# eiscathq disk_archive mariaDB interface:
#################################################

tape_tables = {
'experiments': '''
  experiment_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
  experiment_name varchar(255) NOT NULL,
  country char(2) NULL,
  antenna char(3) NOT NULL,
  comment blob,
  UNIQUE (experiment_name, antenna)
''',
'resource': '''
  resource_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
  experiment_id int NOT NULL,
  start datetime NOT NULL,
  end datetime NOT NULL DEFAULT '1970-01-01',
  comment blob,
  type enum('data', 'info') NOT NULL,
  account varchar(99),
  UNIQUE (experiment_id, start, end, type),
  INDEX (start)
''',
'storage': '''
  location varchar(255) NOT NULL PRIMARY KEY,
  resource_id int NOT NULL,
  priority int NOT NULL DEFAULT 50,
  bytes bigint unsigned,
  comment blob,
  INDEX (resource_id)
''',
'tape_comments': '''
  tape_nr int NOT NULL PRIMARY KEY UNIQUE,
  comment blob
''',
}

class nicedict(dict):
  """A dictionary that behaves like an object, i.e. you can access the
  variables as direct members"""
  def __getattr__(self, attr):
    return self[attr]
  def __setattr__(self, attr, value):
    self[attr] = value

class Conn:
  def __init__(self, dbi, conn):
    """Wraps a database connection with eiscat specific methods.
    dbi is the database library module and
    conn is the connection."""

    assert dbi.paramstyle == 'format', "%s is used for value substitution"
    self.dbi, self.conn = dbi, conn
    self.cur = conn.cursor()

  def select_sql(self, sql, limit=None):
    c = self.cur
    if limit:
      sql += " LIMIT %d"%limit
    c.execute(sql)
    valuess = c.fetchall()
    arry = []
    for values in valuess:
      dict = nicedict()
      for col_info, value in zip(c.description, values):
        name = col_info[0]
        dict[name] = value
      arry.append(dict)
    return arry

  def select_storage_resource_experiments(self, what="*", year = '', limit=None):
    sql = "SELECT "+what+" FROM resource, storage, experiments WHERE "\
          "resource.resource_id = storage.resource_id AND "\
          "resource.experiment_id = experiments.experiment_id AND storage.location like '%%%s%%'" % year
    return self.select_sql(sql, limit=limit)

  def close(self):
    "Closes the connection. The object is unusable after this call."
    self.conn.commit()
    self.conn.close()
    del self.conn
    del self.dbi

def openMySQL(**params):
  import MySQLdb
  db=MySQLdb.connect(**params)
  return Conn(MySQLdb, db)

def datetime2timestamp(t):
  return (t - datetime.datetime(1970,1,1)).total_seconds()

def timestamp2UTC(t):
  return datetime.datetime.utcfromtimestamp(t)


# TODO: more generic a single function to add meta with dictionary and casting prints

def addDatetimeMeta2lfn(catalog, meta_name, meta_value, lfn):
  msg='\nAdding metadata %s=%s to lfn folder %s' % (meta_name, meta_value, lfn)
  print msg
  metadict = {}
  metadict[meta_name]=meta_value
  result=catalog.setMetadata(lfn,metadict)
  if not result['OK']:
    print ("Error: %s" % result['Message'])
  return result

def addstrMeta2lfn(catalog, meta_name, meta_value, lfn):
        global updatemeta
        if updatemeta:
          msg='\nUpdating metadata %s=%s to folder %s' % (meta_name, meta_value, lfn)
          print msg
          catalog.removeMetadata({lfn: meta_name})
        else:
          msg='\nAdding metadata %s=%s to folder %s' % (meta_name, meta_value, lfn)
          print msg
          meta_cmd='set %s %s %s' % (lfn, meta_name, meta_value)
          catalog.setMetadata(lfn, {meta_name, meta_value})
        return result

def registerFiles(catalog, fileDict):
    """ Add a list of file to the catatlog

        usage: add file dict by <filelfn> of <filepfn> <size> <SE>
    """
    print "Bulk file register: ", fileDict
    try:
      result = catalog.addFile(fileDict, timeout=1200 )
      if not result['OK']:
        print "Error: Failed to add file(s) to the catalog: ",
        print result['Message']
      elif result['Value']['Failed']:
        for lfnkey in fileDict:
          if result['Value']['Failed'].has_key(lfnkey):
            print 'Error: Failed to add file: ',lfnkey
      elif result['Value']['Successful']:
        for lfnkey in fileDict:
          if result['Value']['Successful'].has_key(lfnkey):
            print 'File successfully added to the catalog: ',lfnkey
    except Exception, x:
      print "Error: add file(s) failed: ", str(x)


def recursive_chmod(catalog, path, mode, recursive=False):
    """ Do chmod from the given path directory
    """
    lfn = catalog.getPath(path)
    pathDict = {}
    # treat mode as octal
    pathDict[lfn] = eval('0'+mode)

    try:
      result = catalog.fc.changePathMode( pathDict, recursive, timeout=1200 )
      if not result['OK']:
        print "Error chmod:",result['Message']
      if lfn in result['Value']['Failed']:
        print "Error chmod:",result['Value']['Failed'][lfn]
    except Exception, x:
      print "Exception:", str(x)

def recursive_chgrp(catalog, path, account):
    """ Do a recursive chgrp from the given path directory, expecting subdirectory for account
    """
    recursive = True
    lfn = catalog.getPath(path)
    pathDict = {}
    # treat mode as octal
    pathDict[lfn] = account

    try:
      result = catalog.fc.changePathGroup( pathDict, recursive, timeout=1200 )
      if not result['OK']:
        print "Error chgroup:",result['Message']
      if lfn in result['Value']['Failed']:
        print "Error chgroup:",result['Value']['Failed'][lfn]
    except Exception, x:
      print "Exception:", str(x)

def recursive_chown(catalog, path):
    """ Do a recursive chown to eiscat_owner, in regular basis path shall be the year root path of current population (the last operation)
    """
    recursive = True
    lfn = catalog.getPath(path)
    pathDict = {}
    # treat mode as octal
    pathDict[lfn] = 'eiscat_owner'

    try:
      result = catalog.fc.changePathOwner( pathDict, recursive, timeout=1200 )
      if not result['OK']:
        print "Error chgroup:",result['Message']
      if lfn in result['Value']['Failed']:
        print "Error chgroup:",result['Value']['Failed'][lfn]
    except Exception, x:
      print "Exception:", str(x)


def registerFilesTree(catalog,lfn,pfn):
  global processedFiles
  fileDict = {}
  dirlist = os.listdir(pfn)
  for entry in dirlist:
    entrypfn="%s/%s" % (pfn, entry)
    entrylfn="%s/%s" % (lfn, entry)
    if os.path.isdir(entrypfn):
      registerFilesTree(catalog, entrylfn, entrypfn)
      continue
    infoDict = {}
    infoDict['PFN'] = entrypfn
    try:
      size = os.fstat(entrypfn).ST_SIZE
      infoDict['Size'] = int(size)
    except Exception:
      print "Setting size 0, because error getting ls -l "+entrypfn
      infoDict['Size'] = 0
    infoDict['SE'] = 'EISCAT-disk'
    # make a UUID based on the host ID and current time
    #infoDict['GUID'] = uuid.uuid1().int
    infoDict['GUID'] = str(uuid.uuid1().hex)
    infoDict['Checksum'] = ''
    if registerChecksum:
      infoDict['Checksum'] = str(fileAdler(entrypfn))

    fileDict[entrylfn] = infoDict

  # bulk files registration in the current level
  if fileDict:
    print "Registering %d files in %s directory" % (len(fileDict), pfn)
    registerFiles(catalog, fileDict)
    processedFiles += len(fileDict)

########################################################################
# initialize catalog
########################################################################

result = FileCatalogFactory().createCatalog('EiscatFileCatalog')
if not result['OK']:
  print "Failed to create %s client " % fcType
  sys.exit(1)
print "Starting %s DIRAC client" % fcType
catalog = result['Value']

########################################################################
# search db, register files and metadata key values to folders
########################################################################

fieldList = ["storage.location",
             "storage.bytes",
             "resource.start",
             "resource.end",
             "resource.type",
             "resource.account",
             "experiments.experiment_name",
             "experiments.country",
             "experiments.antenna"]

dbclient = openMySQL(host="eiscathq.eiscat.se", db='disk_archive', user='www')
list_entries = dbclient.select_storage_resource_experiments(what=",".join(fieldList),
                                                            year=listyear)
dbclient.close()

if pbar:
  import progressbar
  ne=len(list_entries)
  pb=progressbar.ProgressBar(widgets=[progressbar.Bar(), progressbar.Percentage()], maxval=ne).start()
  n=0

processed=[]
processedFiles = 0
for entry in list_entries:
  newFolder = True
  if listyear in entry['location']:
    tier_folder = listyear + entry['location'].split(listyear)[1]
    pfn=root_pfn+tier_folder
    lfn=root_lfn+tier_folder
    if os.path.isdir(pfn):

      # if pfn already processed (some entries are duplicated in DB)
      if pfn in processed:
        continue
      processed.append(pfn)

      print "*******************************************************************************"
      print pfn

      # if lfn was previously populated in the catalog
      result =  catalog.isDirectory(lfn)
      if result['OK']:
        if result['Value']['Successful']:
          if result['Value']['Successful'][lfn]:
            print "Already in catalogue"
            newFolder = False
            if updatemeta:
              print "Updating metadata"
            else:
              continue

      # prepare metadata logic before registering new files of the tier folder

      # get parent tree of current tier folder
      level_list=tier_folder.split('/')

      lfn_level1='%s/%s' % (root_lfn,level_list[1])
      # folder level1: no metadata

      lfn_level2='%s/%s' % (lfn_level1,level_list[2])
      # folder level2: experiment

      lfn_level3='%s/%s' % (lfn_level2,level_list[3])
      # folder level3: if type data : level 4 -> DATAFILE -> DATA
      # folder level3: if type info : level 4 -> date ; level 5 -> DATAFILE -> DATA
      # folder level3: if type info : level 4 -> date ; level 5 -> time ; level 6 -> DATAFILE -> DATA

      # before registering new files, check if parent directories are also new in the catalogue, so needing metadata creation latter
      # initialize metadata_level
      metadata_level2 = True
      metadata_level3 = True
      result =  catalog.isDirectory(lfn_level3)
      if result['OK']:
        if result['Value']['Successful']:
          if result['Value']['Successful'][lfn_level3]:
            metadata_level3 = False
      result =  catalog.isDirectory(lfn_level2)
      if result['OK']:
        if result['Value']['Successful']:
          if result['Value']['Successful'][lfn_level2]:
            metadata_level2 = False


      if newFolder:
        # bulk files registering of a tree (this creates parent directories if not existing before)
        registerFilesTree(catalog, lfn, pfn)

      #adding/updating metadata to the corresponding folder level
      #location, bytes, start, end, type, account, experiment_name, country, antenna
      if metadata_level2 == True:
        # tier_folder level2: experiment
        addstrMeta2lfn(catalog, 'experiment_name', entry['experiment_name'], lfn_level2)
        addstrMeta2lfn(catalog, 'country', entry['country'], lfn_level2)
        addstrMeta2lfn(catalog, 'antenna', entry['antenna'], lfn_level2)
      if metadata_level3 == True:
        addstrMeta2lfn(catalog, 'type', entry['type'], lfn_level3)

      #at this point the rest of metadata are in going to the tier_folder, whatever level of DATAFILE folder in lfn:
      # tier_folder is DATAFILE folder; level3: if type data : level 4 -> DATAFILE -> DATA
      # tier_folder is DATAFILE folder; level3: if type info : level 4 -> date ; level 5 -> DATAFILE -> DATA
      # tier_folder is DATAFILE folder; level3: if type info : level 4 -> date ; level 5 -> time ; level 6 -> DATAFILE -> DATA
      # bytes, start, end, account
      # bytes, start, end are not meta index index (search), just metadata(show), added as string
      addstrMeta2lfn(catalog, 'bytes', str(entry['bytes']), lfn)
      metastart = str(entry['start']).replace(" ","/")
      addstrMeta2lfn(catalog, 'start', metastart, lfn)
      if entry['end'] > entry['start']:
        # Exp stop time is defined
        metaend = str(entry['end']).replace(" ","/")
      else:
        # until V8: same as metastart
        metaend = metastart
      addstrMeta2lfn(catalog, 'end', metaend, lfn)
      addstrMeta2lfn(catalog, 'account', entry['account'], lfn)

    else:
      errmsg="Error: sql pfn %s is not a filesystem directory." % pfn
      print errmsg

  if pbar:
    n=n+1
    pb.update(n)

year_lfn="%s%s" % (root_lfn,listyear)
recursive_chgrp(catalog, year_lfn, 'eiscat_files')
recursive_chmod(catalog, year_lfn, '750', True)
recursive_chmod(catalog, year_lfn, '755', False)
# eiscat_owner recursively
recursive_chown(catalog, year_lfn)

#dbclient.close()
lgf.close()

if pbar:
  pb.finish()
###########################
