"""
   EiscatPolicy defines File Catalog data access policies specific
   for the Eiscat 3D community
"""

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getAllGroups, getGroupOption
from DIRAC.DataManagementSystem.DB.FileCatalogComponents.SecurityManager import SecurityManagerBase, _readMethods, _writeMethods
from DIRAC.Resources.Catalog.FileCatalogFactory import FileCatalogFactory
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult

import os
import datetime

__RCSID__ = "$Id$"

class EiscatPolicy( SecurityManagerBase ):
  """ This class implements an eiscat policy, connecting remote eiscat catalog
  """

  def __init__( self, database = False ):
    super( EiscatPolicy, self ).__init__( database = database )

    self.logger = gLogger.getSubLogger( 'EiscatPolicy' )

    # voms role : [dirac groups that have it]
    self.vomsRoles = {}
    # dirac group : voms role it has
    self.diracGroups = {}

    fcType = 'EiscatFileCatalog'
    result = FileCatalogFactory().createCatalog(fcType)
    if not result['OK']:
      self.logger.error( 'EiscatFileCatalog not found' )
      return
    self.fc = result['Value']
    # Lifetime of the info in the two dictionaries
    self.CACHE_TIME = datetime.timedelta(seconds = 600)
    self.__buildRolesAndGroups()


    ########################################
    # IMPORTANT globals
    ########################################
    self.now = datetime.datetime.now()
    self.lastyear=self.now.year - 1
    #for testing:
    #self.lastyear=self.now.year - 2
    self.lastmonth=self.now.month - 1

  def __datetime2timestamp(t):
	return (t-datetime.datetime(1970,1,1)).total_seconds()

  def __timestamp2UTC(t):
	return datetime.datetime.utcfromtimestamp(t)


  def __buildRolesAndGroups( self ):
    """ Rebuild the cache dictionary for VOMS roles and EiscatDIRAC Groups"""

    self.lastBuild = datetime.datetime.now()

    allGroups = getAllGroups()


    for grpName in allGroups:
      vomsRole = getGroupOption( grpName, 'VOMSRole' )
      if vomsRole:
        self.diracGroups[grpName] = vomsRole
        self.vomsRoles.setdefault( vomsRole, [] ).append( grpName )


  def __getVomsRole(self, grpName):
    """ Returns the VOMS role of a given EiscatDIRAC group

        :param grpName:

        :returns: VOMS role, or None
    """
    if ( datetime.datetime.now() - self.lastBuild ) > self.CACHE_TIME:
      self.__buildRolesAndGroups()

    return self.diracGroups.get( grpName )

  def __getDiracGroups( self, vomsRole ):
    """ Returns all the EiscatDIRAC groups that have a given VOMS role

        :param vomsRole:

        :returns: list of groups, empty if not exist
    """

    if ( datetime.datetime.now - self.lastBuild ) > self.CACHE_TIME:
      self.__buildRolesAndGroups()

    return self.vomsRoles.get( vomsRole, [] )

  def __shareVomsRole( self, grpName, otherGrpName ):
    """ Returns True if the two EiscatDIRAC groups have the same VOMS role"""

    vomsGrp = self.__getVomsRole( grpName )
    vomsOtherGrp = self.__getVomsRole( otherGrpName )
    # The voms group cannot be None
    return  vomsGrp and vomsOtherGrp and ( vomsGrp == vomsOtherGrp )

  def __eiscatFilesMatching( self, path, credDict, origGrp, isDir = None ):
    """ Returns original credDict of the esicat_files if matchs eiscat policy with metadata catalogue info """

    if isDir is None:
      self.logger.info( 'EiscatFilesMatching needs isDir, no eiscat rules matching' )
      return credDict

    if origGrp == 'eiscat_files':
      self.logger.info( 'eiscat rules to path %s ' % path )
      # get user credentials in group access:
      credGroup = credDict.get( 'group', 'anon' )

      if isDir is True:
        rpcClient = self.fc._getRPC( timeout = 120 )
        result = rpcClient.getDirectoryUserMetadata( path )
        if not result['OK']:
          self.logger.error( 'Error in getDirectoryUserMetadata, no eiscat rules %s ' % (result) )
          return credDict
      else:
        directory = "/".join( path.split( "/" )[:-1] )
        rpcClient = self.fc._getRPC( timeout = 120 )
        result = rpcClient.getFileUserMetadata( path )
        if not result['OK']:
          self.logger.error( 'Error in getFileUserMetadata, no eiscat rules %s ' % (result) )
          return credDict
        fmeta = result['Value']
        result = rpcClient.getDirectoryUserMetadata( directory )
        if not result['OK']:
          self.logger.error( 'Error in getDirectoryUserMetadata, no eiscat rules %s ' % (result) )
          return credDict
        fmeta.update(result['Value'])
        result=S_OK(fmeta)

      self.logger.info( 'Result %s ' % (result) )

      if not result['Value']:
        self.logger.info( 'Error getting Value of entry %s in Eiscat Catalogue, no eiscat rules ' % (path) )
        return credDict
      entry=result['Value']

      self.logger.info( 'Path %s  metadata entry in Eiscat Catalogue %s ' % (path,entry) )

      if 'start' in entry:
        strdate=entry['start']
        refdate = datetime.datetime.strptime(strdate, '%Y-%m-%d/%H:%M:%S')
        year=int(refdate.year)
        month=int(refdate.month)
      else:
	#nostart means no restriction in the access to the file/folder
        #actually, all the files shall has date and only intermediate folders could not have date, so enable reading the dir
        self.logger.info( 'No start date is access same as more than a year')
        year=2000
        month=1

      if not ((year > self.lastyear) or (year == self.lastyear and month > self.lastmonth)):
        credDict = { 'username' : credDict.get( 'username', 'anon' ), 'group' : origGrp}
        self.logger.info( 'Grant public access (%s): more than a year  ' % credDict)
        return credDict

      self.logger.info( 'Less than a year rules')
      # this is be used for groups when files less than a year
      tag_country = None
      if 'country' in entry:
        if not entry['country'] == 'None':
          tag_country=str(entry['country'])
          self.logger.info( 'Initial tag_country %s ' % tag_country)
          if tag_country is 'NI':
            tag_country='JP'
          elif tag_country is 'SW':
            tag_country='SE'
          elif tag_country is 'GE':
            tag_country='DE'
          elif tag_country is 'SP':
            tag_country='owner'
          elif tag_country in ['CP','AA', 'G4', 'G2', 'G7']:
            tag_country='common'

      self.logger.info( 'After rules tag_country %s ' % tag_country)

      tag_account = None
      if 'account' in entry:
        if not entry['account'] == 'None':
          tag_account=[]
          tag_account.append(entry['account'])
          if entry['account'].find('NI'):
            tag_account.append('JP')
          if entry['account'].find('SW'):
            tag_account.append('SE')
          if entry['account'].find('GE'):
            tag_account.append('DE')
          if entry['account'].find('SP'):
            tag_account.append('owner')
          if entry['account'].find('CP'):
            tag_account.append('common')

      self.logger.info( 'After rules tag_account %s ' % tag_account)
      self.logger.info( 'Going to match with request group: %s ' % credGroup)

      postag=credGroup.find('eiscat_')
      if postag == -1:
        self.logger.info( 'No eiscat_ like group, %s, no eiscat rules access ' % credGroup)
        return credGroup

      pos=postag+len('eiscat_')
      tagCredGroup=credGroup[pos:]
      self.logger.info( 'tag of CredGroup to match in account/country %s' % tagCredGroup)

      if tag_account is None:
        if tag_country is None:
          credDict = { 'username' : credDict.get( 'username', 'anon' ), 'group' : origGrp}
          self.logger.info( 'Grant public access (%s): no account and no country  ' % credDict)
          return credDict
        else:
          if tag_country == 'common':
            credDict = { 'username' : credDict.get( 'username', 'anon' ), 'group' : origGrp}
            self.logger.info( 'Grant public access (%s): country  is common ' % credDict)
            return credDict
          if tagCredGroup == tag_country:
            credDict = { 'username' : credDict.get( 'username', 'anon' ), 'group' : origGrp}
            self.logger.info( 'Grant public access (%s): tagCredGroup %s is tag_country %s ' %(credDict, tagCredGroup, tag_country))
            return credDict
      else:
        for tag in tag_account:
          if tag.find(tagCredGroup):
            credDict = { 'username' : credDict.get( 'username', 'anon' ), 'group' : origGrp}
            self.logger.info( 'Grant public access (%s): tagCredGroup %s is tag_account %s ' %(credDict, tagCredGroup, tag_account))
            return credDict

    self.logger.info( 'No eiscat rules to Public Grant, let user credentials' )
    return  credDict


  def __isNotExistError( self, errorMsg ):
    """ Returns true if the errorMsg means that the file/directory does not exist """

    for possibleMsg in ['not exist', 'not found', 'No such file or directory']:
      if possibleMsg in errorMsg:
        return True

    return False





  def __getFilePermission( self, path, credDict, noExistStrategy = None ):
    """ Checks POSIX permission for a file using the VOMS roles.
        That is, if the owner group of the file shares the same vomsRole as the requesting user,
        we check the permission as if the request was done with the real owner group.

        :param path : file path (string)
        :param credDict : credential of the user
        :param noExistStrategy : If the directory does not exist, we can
                                 * True : allow the access
                                 * False : forbid the access
                                 * None : return the error as is

        :returns S_OK structure with a dictionary ( Read/Write/Execute : True/False)
    """


    if not path:
      return S_ERROR( 'Empty path' )

    # We check what is the group stored in the DB for the given path
    res = returnSingleResult( self.db.fileManager.getFileMetadata( [path] ) )
    if not res['OK']:
      # If the error is not due to the directory not existing, we return
      if not self.__isNotExistError( res['Message'] ):
        return res

      # From now on, we know that the error is due to the file not existing

      # If we have no strategy regarding non existing files, then just return the error
      if noExistStrategy is None:
        return res

      # Finally, follow the strategy
      return S_OK( dict.fromkeys( ['Read', 'Write', 'Execute'], noExistStrategy ) )

    #===========================================================================
    # # This does not seem necessary since we add the OwnerGroup in the query behind the scene
    # origGrp = 'unknown'
    # res = self.db.ugManager.getGroupName( res['Value']['GID'] )
    # if res['OK']:
    #   origGrp = res['Value']
    #===========================================================================

    origGrp = res['Value'].get( 'OwnerGroup', 'unknown' )

    # If the two group share the same voms role, we do the query like if we were
    # the group stored in the DB
    # useless in eiscat
    # if self.__shareVomsRole( credDict.get( 'group', 'anon' ), origGrp ):
    #   credDict = { 'username' : credDict.get( 'username', 'anon' ), 'group' : origGrp}

    # eiscat rules,
    credDict = self.__eiscatFilesMatching( path, credDict, origGrp, isDir = False )

    return  returnSingleResult( self.db.fileManager.getPathPermissions( [path], credDict ) )


  def __testPermissionOnFile( self, paths, permission, credDict, noExistStrategy = None ):
    """ Tests a permission on a list of files

        :param path : list/dict of file paths
        :param permission : Read/Write/Execute string
        :param credDict : credential of the user
        :param noExistStrategy : If the directory does not exist, we can
                                 * True : allow the access
                                 * False : forbid the access
                                 * None : return the error as is

        :returns: Successful dictionary with True of False, and Failed.
    """

    successful = {}
    failed = {}

    for filename in paths:
      res = self.__getFilePermission( filename, credDict, noExistStrategy = noExistStrategy )
      if not res['OK']:
        failed[filename] = res['Message']
      else:
        successful[filename] = res['Value'].get( permission, False )

    return S_OK( { 'Successful' : successful, 'Failed' : failed } )



  def __getDirectoryPermission( self, path, credDict, recursive = True, noExistStrategy = None ):
    """ Checks POSIX permission for a directory using the VOMS roles.
        That is, if the owner group of the directory share the same vomsRole as the requesting user,
        we check the permission as if the request was done with the real owner group.

        :param path : directory path (string)
        :param credDict : credential of the user
        :param recursive : if that directory does not exist, checks the parent one
        :param noExistStrategy : If the directory does not exist, we can
                                 * True : allow the access
                                 * False : forbid the access
                                 * None : return the error as is

               noExistStrategy makes sense only if recursive is False

        :returns S_OK structure with a dictionary ( Read/Write/Execute : True/False)
    """

    if not path:
      return S_ERROR( 'Empty path' )

    # We check what is the group stored in the DB for the given path
    res = self.db.dtree.getDirectoryParameters( path )
    if not res['OK']:
      # If the error is not due to the directory not existing, we return

      if not self.__isNotExistError( res['Message'] ):
        return res

      # Very special case to allow creation of very first entry
      if path == '/':
        return S_OK( {'Read':True, 'Write':True, 'Execute':True} )

      # From now on, we know that the error is due to the directory not existing

      # If recursive, we try the parent directory
      if recursive:
        return self.__getDirectoryPermission( os.path.dirname( path ), credDict,
                                             recursive = recursive, noExistStrategy = noExistStrategy )
      # From now on, we know we don't run recursive

      # If we have no strategy regarding non existing directories, then just return the error
      if noExistStrategy is None:
        return res

      # Finally, follow the strategy
      return S_OK( dict.fromkeys( ['Read', 'Write', 'Execute'], noExistStrategy ) )

    # The directory exists.
    origGrp = res['Value']['OwnerGroup']

    # If the two group share the same voms role, we do the query like if we were
    # the group stored in the DB
    # useless in eiscat
    #if self.__shareVomsRole( credDict.get( 'group', 'anon' ), origGrp ):
    #  credDict = { 'username' : credDict.get( 'username', 'anon' ), 'group' : origGrp}

    # eiscat rules,
    credDict = self.__eiscatFilesMatching( path, credDict, origGrp, isDir = True )

    return  self.db.dtree.getDirectoryPermissions( path, credDict )



  def __testPermissionOnDirectory( self, paths, permission, credDict, recursive = True, noExistStrategy = None ):
    """ Tests a permission on a list of directories

        :param path : list/dict of directory paths
        :param permission : Read/Write/Execute string
        :param credDict : credential of the user
        :param recursive : if that directory does not exist, checks the parent one
        :param noExistStrategy : If the directory does not exist, we can
                                 * True : allow the access
                                 * False : forbid the access
                                 * None : return the error as is

               noExistStrategy makes sense only if recursive is False

        :returns: Successful dictionary with True of False, and Failed.
    """

    successful = {}
    failed = {}

    for dirName in paths:
      # to list folders by eiscat policy:
      res = self.db.dtree.getDirectoryParameters( dirName )
      if res['OK']:
        origGrp = res['Value']['OwnerGroup']
        credDict = self.__eiscatFilesMatching( dirName, credDict, origGrp, isDir = True )
      res = self.__getDirectoryPermission( dirName, credDict, recursive = recursive, noExistStrategy = noExistStrategy )
      if not res['OK']:
        failed[dirName] = res['Message']
      else:
        successful[dirName] = res['Value'].get( permission, False )

    return S_OK( { 'Successful' : successful, 'Failed' : failed } )


  def __testPermissionOnParentDirectory( self, paths, permission, credDict, recursive = True, noExistStrategy = None ):
    """ Tests a permission on the parents of a list of directories

        :param path : directory path (string)
        :param permission : Read/Write/Execute string
        :param credDict : credential of the user
        :param recursive : if that directory does not exist, checks the parent one
        :param noExistStrategy : If the directory does not exist, we can
                                 * True : allow the access
                                 * False : forbid the access
                                 * None : return the error as is

               noExistStrategy makes sense only if recursive is False

        :returns: Successful dictionary with True of False, and Failed.
    """

    parentDirs = {}
    for path in paths:
      parentDirs.setdefault( os.path.dirname( path ), [] ).append( path )

    res = self.__testPermissionOnDirectory( parentDirs, permission, credDict,
                                            recursive = recursive, noExistStrategy = noExistStrategy )

    if not res['OK']:
      return res

    failed = res['Value']['Failed']
    successful = {}

    parentAllowed = res['Value']['Successful']

    for parentName in parentAllowed:
      isParentAllowed = parentAllowed[parentName]
      for path in parentDirs[parentName]:
        successful[path] = isParentAllowed

    return S_OK( { 'Successful' : successful, 'Failed' : failed } )



  def __getFileOrDirectoryPermission( self, path, credDict, recursive = False, noExistStrategy = None ):
    """ Checks POSIX permission for a directory or file using the VOMS roles.
        That is, if the owner group of the directory or file shares the same vomsRole as the requesting user,
        we check the permission as if the request was done with the real owner group.

        We first consider the path as a file, and if it does not exist, we consider it as a directory.

        :param path : directory or file path (string)
        :param credDict : credential of the user
        :param recursive : if that directory does not exist, checks the parent one
        :param noExistStrategy : If the directory does not exist, we can
                                 * True : allow the access
                                 * False : forbid the access
                                 * None : return the error as is

               noExistStrategy makes sense only if recursive is False

        :returns S_OK structure with a dictionary ( Read/Write/Execute : True/False)
    """
    #First consider it as File
    # We want to know whether the file does not exist, so we force noExistStrategy to None
    res = self.__getFilePermission( path, credDict, noExistStrategy = None )
    if not res['OK']:
      # If the error is not due to the directory not existing, we return
      if not self.__isNotExistError( res['Message'] ):
        return res

      # From now on, we know that the error is due to the File not existing
      # We Try then the directory method, since path can be a directory
      # The noExistStrategy will be applied by __getDirectoryPermission, so we don't need to do it ourselves
      res = self.__getDirectoryPermission( path, credDict, recursive = recursive, noExistStrategy = noExistStrategy )

    return res


  def __testPermissionOnFileOrDirectory( self, paths, permission, credDict, recursive = False, noExistStrategy = None ):
    """ Tests a permission on a list of files or directories.

        :param path : list/dict of directory or files paths
        :param permission : Read/Write/Execute string
        :param credDict : credential of the user
        :param recursive : if that directory does not exist, checks the parent one
        :param noExistStrategy : If the directory does not exist, we can
                                 * True : allow the access
                                 * False : forbid the access
                                 * None : return the error as is

               noExistStrategy makes sense only if recursive is False

        :returns: Successful dictionary with True of False, and Failed.
    """

    successful = {}
    failed = {}

    for path in paths:
      res = self.__getFileOrDirectoryPermission( path, credDict, recursive = recursive, noExistStrategy = noExistStrategy )
      if not res['OK']:
        failed[path] = res['Message']
      else:
        successful[path] = res['Value'].get( permission, False )

    return S_OK( { 'Successful' : successful, 'Failed' : failed } )



  def __policyRemoveDirectory( self, paths, credDict ):
    """ Tests whether the remove operation on directories
        is permitted.
        Removal of non existing directory is always allowed.
        For existing directories, we must have the write permission
        on the parent

        :param paths : list/dict of path
        :param credDict : credential of the user
        :returns: Successful with True of False, and Failed.
    """

    successful = {}

    # We allow removal of all the non existing directories
    res = self.db.dtree.exists( paths )
    if not res['OK']:
      return res

    nonExistingDirectories = set( path for path in res['Value']['Successful'] if not res['Value']['Successful'][path] )

    existingDirs = set( paths ) - set( nonExistingDirectories )
    for dirName in nonExistingDirectories:
      successful[dirName] = True

    res = self.__testPermissionOnParentDirectory( existingDirs, 'Write', credDict, recursive = False )
    if not res['OK']:
      return res

    failed = res['Value']['Failed']
    successful.update(res['Value']['Successful'])

    return S_OK( { 'Successful' : successful, 'Failed' : failed } )



  def __policyRemoveFile( self, paths, credDict ):
    """ Tests whether the remove operation on files
        is permitted.
        Removal of non existing file is always allowed.
        For existing files, we must have the write permission
        on the parent

        :param paths : list/dict of path
        :param credDict : credential of the user
        :returns: Successful with True of False, and Failed.
    """

    successful = {}

    # We allow removal of all the non existing files
    res = self.db.fileManager.exists( paths )
    if not res['OK']:
      return res

    nonExistingFiles = set( path for path in res['Value']['Successful'] if not res['Value']['Successful'][path] )

    existingFiles = set( paths ) - set( nonExistingFiles )
    for dirName in nonExistingFiles:
      successful[dirName] = True

    res = self.__testPermissionOnParentDirectory( existingFiles, 'Write', credDict, recursive = False )
    if not res['OK']:
      return res

    failed = res['Value']['Failed']
    successful.update( res['Value']['Successful'] )

    return S_OK( { 'Successful' : successful, 'Failed' : failed } )




  def __policyListDirectory( self, paths, credDict ):
    """ Test Read permission on the directory.
        If the directory does not exist, we do not allow.

        :param paths : list/dict of path
        :param credDict : credential of the user
        :returns: Successful with True of False, and Failed.
    """

    return self.__testPermissionOnDirectory( paths, 'Read', credDict,
                                                   recursive = True )



  def __policyReadForFileAndDirectory( self, paths, credDict ):
    """ Testing the read bit on the parent directory,
        be it a file or a directory.
        So it reads permissions from a directory

        :param paths : list/dict of path
        :param credDict : credential of the user
        :returns: Successful with True of False, and Failed.
    """

    return self.__testPermissionOnParentDirectory( paths, 'Read', credDict,
                                                   recursive = True )

  def __policyWriteForFileAndDirectory( self, paths, credDict ):
    """ Testing the read bit on the parent directory (recursively),
        be it a file or a directory.
        So it reads permissions from a directory

        :param paths : list/dict of path
        :param credDict : credential of the user
        :returns: Successful with True of False, and Failed.
    """

    return self.__testPermissionOnParentDirectory( paths, 'Write', credDict,
                                                   recursive = True )


  def __policyReadForReplica( self, paths, credDict ):
    """ Test Read permission on the file associated to the replica.
        If the file does not exist, we allow.

        :param paths : list/dict of path
        :param credDict : credential of the user
        :returns: Successful with True of False, and Failed.
    """
    return self.__testPermissionOnFile( paths, 'Read', credDict,
                                               noExistStrategy = True )

  def __policyWriteForReplica( self, paths, credDict ):
    """ Test Write permission on the file associated to the replica.
        If the file does not exist, we allow.

        :param paths : list/dict of path
        :param credDict : credential of the user
        :returns: Successful with True of False, and Failed.
    """
    return self.__testPermissionOnFile( paths, 'Write', credDict,
                                               noExistStrategy = True )

  def __policyWriteOnFile( self, paths, credDict ):
    """ Test Write permission on the file.
        If the file does not exist, we allow.

        :param paths : list/dict of path
        :param credDict : credential of the user
        :returns: Successful with True of False, and Failed.
    """
    return self.__testPermissionOnFile( paths, 'Write', credDict,
                                               noExistStrategy = True )



  def __policyChangePathMode( self, paths, credDict ):
    """ Test Write permission on the directory/file.
        If the directory/file does not exist, we allow.

        :param paths : list/dict of path
        :param credDict : credential of the user
        :returns: Successful with True of False, and Failed.
    """

    return self.__testPermissionOnFileOrDirectory( paths, 'Write', credDict,
                                                   recursive = False, noExistStrategy = True )


  def __policyDeny( self, paths, credDict ):
    """ Denies the access to all the paths given

        :param paths : list/dict of path
        :param credDict : credential of the user
        :returns: Successful with True of False, and Failed.
    """

    return S_OK( {'Successful': dict.fromkeys( paths, False ), 'Failed' : {}} )


#
# __writeMethods = ['setMetadata','__removeMetadata']


  def hasAccess( self, opType, paths, credDict ):
    """ Checks whether a given operation on given paths is permitted

        :param opType : name of the operation (the FileCatalog methods in fact...)
        :param paths: list/dictionary of path on which we want to apply the operation
        :param credDict : credential of the users (with at least username, group and properties)

        :returns: Successful dict with True or False, and Failed dict. In fact, it is not neccesarily
                a boolean, rather an int (binary operation results)
    """
    # Check if admin access is granted first
    result = self.hasAdminAccess( credDict )
    if not result['OK']:
      return result

    if result['Value']:
      # We are admin, allow everything
      return S_OK( {'Successful':dict.fromkeys( paths, True ), 'Failed':{}} )


    if opType not in _readMethods + _writeMethods:
      return S_ERROR( "Operation type not known %s" % opType )


    if self.db.globalReadAccess and ( opType in _readMethods ):
      return S_OK( {'Successful':dict.fromkeys( paths, True ), 'Failed':{}} )

    policyToExecute = None

    if opType == 'removeDirectory':
      policyToExecute = self.__policyRemoveDirectory

    elif opType in ['createDirectory', 'addFile']:
      policyToExecute = self.__policyWriteForFileAndDirectory

    elif opType == 'removeFile':
      policyToExecute = self.__policyRemoveFile

    elif opType in ['setFileMode', 'addFileAncestors', 'setFileStatus', 'addReplica',
                     'removeReplica', 'setReplicaStatus', 'setReplicaHost']:
      policyToExecute = self.__policyWriteOnFile

    elif opType == 'listDirectory':
      policyToExecute = self.__policyListDirectory

    elif opType in ['isDirectory', 'getDirectoryReplicas', 'getDirectoryMetadata', 'getDirectorySize',
                     'isFile', 'getFileSize', 'getFileMetadata', 'exists',
                     'getFileAncestors', 'getFileDescendents']:
      policyToExecute = self.__policyReadForFileAndDirectory

    elif opType in ['getReplicas', 'getReplicaStatus']:
      policyToExecute = self.__policyReadForReplica

    # Only admin can do that, and if we are here, we are not admin
    elif opType in ['changePathOwner', 'changePathGroup', 'setFileOwner', 'setFileGroup']:
      policyToExecute = self.__policyDeny

    elif opType == 'changePathMode':
      policyToExecute = self.__policyChangePathMode

    if not policyToExecute:
      return S_ERROR( "No policy matching operation %s" % opType )

    res = policyToExecute( paths, credDict )

    return res


  def getPathPermissions( self, paths, credDict ):
    """ This method is meant to disappear, hopefully soon,
        but as long as we have clients from versions < v6r14,
        we need a getPathPermissions method. Since it does not make
        sense with that kind of fine grain policy, we return what used to
        be returned...
    """

    oldSEM = getattr( self, 'oldSecurityManager', None )
    if oldSEM:
      return oldSEM.getPathPermissions( paths, credDict )
    else:
      return super( EiscatPolicy, self ).getPathPermissions( paths, credDict )
