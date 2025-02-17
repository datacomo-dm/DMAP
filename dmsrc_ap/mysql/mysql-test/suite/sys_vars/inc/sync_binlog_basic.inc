#################### mysql-test\t\sync_binlog_basic.test ######################
#                                                                             #
# Variable Name: sync_binlog                                                  #
# Scope: GLOBAL                                                               #
# Access Type: Dynamic                                                        #
# Data Type: numeric                                                          #
# Default Value: 0                                                            #
# Range: 0 - 4294967295                                                       #
#                                                                             #
#                                                                             #
# Creation Date: 2008-02-07                                                   #
# Author:  Salman Rawala                                                      #
#                                                                             #
# Description: Test Cases of Dynamic System Variable sync_binlog              #
#              that checks the behavior of this variable in the following ways#
#              * Default Value                                                #
#              * Valid & Invalid values                                       #
#              * Scope & Access method                                        #
#              * Data Integrity                                               #
#                                                                             #
# Reference: http://dev.mysql.com/doc/refman/5.1/en/                          #
#  server-system-variables.html                                               #
#                                                                             #
###############################################################################

--source include/load_sysvars.inc

##################################################################
#                    START OF sync_binlog TESTS                  #
##################################################################


##################################################################
# Saving initial value of sync_binlog in a temporary variable    #
##################################################################

SET @start_value = @@global.sync_binlog;
SELECT @start_value;


--echo '#--------------------FN_DYNVARS_168_01------------------------#'
##################################################################
#              Display the DEFAULT value of sync_binlog          #
################################################################## 

SET @@global.sync_binlog = 99;
SET @@global.sync_binlog = DEFAULT;
SELECT @@global.sync_binlog;


--echo '#---------------------FN_DYNVARS_168_02-------------------------#'
############################################### 
#     Verify default value of variable        #
############################################### 

SET @@global.sync_binlog = @start_value;
SELECT @@global.sync_binlog = 0;


--echo '#--------------------FN_DYNVARS_168_03------------------------#'
###################################################################
#        Change the value of sync_binlog to a valid value         #
###################################################################

SET @@global.sync_binlog = 0;
SELECT @@global.sync_binlog;
SET @@global.sync_binlog = 1;
SELECT @@global.sync_binlog;
SET @@global.sync_binlog = 4294967295;
SELECT @@global.sync_binlog;
SET @@global.sync_binlog = 4294967294;
SELECT @@global.sync_binlog;
SET @@global.sync_binlog = 65536;
SELECT @@global.sync_binlog;


--echo '#--------------------FN_DYNVARS_168_04-------------------------#'
######################################################################
#         Change the value of sync_binlog to invalid value           #
######################################################################

SET @@global.sync_binlog = -1;
SELECT @@global.sync_binlog;
SET @@global.sync_binlog = 4294967296;
SELECT @@global.sync_binlog;
SET @@global.sync_binlog = 10240022115;
SELECT @@global.sync_binlog;
--Error ER_WRONG_TYPE_FOR_VAR
SET @@global.sync_binlog = 10000.01;
SELECT @@global.sync_binlog;
SET @@global.sync_binlog = -1024;
SELECT @@global.sync_binlog;
SET @@global.sync_binlog = 42949672950;
SELECT @@global.sync_binlog;
--echo 'Bug # 34837: Errors are not coming on assigning invalid values to variable';

--Error ER_WRONG_TYPE_FOR_VAR
SET @@global.sync_binlog = ON;
SELECT @@global.sync_binlog;
--Error ER_WRONG_TYPE_FOR_VAR
SET @@global.sync_binlog = 'test';
SELECT @@global.sync_binlog;


--echo '#-------------------FN_DYNVARS_168_05----------------------------#'
########################################################################### 
#       Test if accessing session sync_binlog gives error            #
########################################################################### 

--Error ER_GLOBAL_VARIABLE
SET @@session.sync_binlog = 0;
SELECT @@sync_binlog;


--echo '#----------------------FN_DYNVARS_168_06------------------------#'
############################################################################## 
# Check if the value in GLOBAL & SESSION Tables matches values in variable   #
##############################################################################

SELECT @@global.sync_binlog = VARIABLE_VALUE
FROM INFORMATION_SCHEMA.GLOBAL_VARIABLES
WHERE VARIABLE_NAME='sync_binlog';



--echo '#---------------------FN_DYNVARS_168_07----------------------#'
##########################################################################  
#   Check if sync_binlog can be accessed with and without @@ sign   #
##########################################################################

--Error ER_GLOBAL_VARIABLE
SET sync_binlog = 1;
SELECT @@sync_binlog;
--Error ER_PARSE_ERROR
SET local.sync_binlog = 1;
--Error ER_UNKNOWN_TABLE
SELECT local.sync_binlog;
--Error ER_PARSE_ERROR
SET global.sync_binlog = 1;
--Error ER_UNKNOWN_TABLE
SELECT global.sync_binlog;
--Error ER_BAD_FIELD_ERROR
SELECT sync_binlog = @@session.sync_binlog;


##############################  
#   Restore initial value    #
##############################

SET @@global.sync_binlog = @start_value;
SELECT @@global.sync_binlog;


######################################################################## 
#              END OF sync_binlog TESTS                           #
######################################################################## 
