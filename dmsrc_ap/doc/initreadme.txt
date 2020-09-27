 
set condition var:
  export CVSROOT=:pserver:wgsh@hostaddr:/cvsroot
login to cvs :
  cvs login
build a empty directory to hold dbplus source files.
   mkdir ttt
get a development copies .
  cd ttt
  cvs checkout dbplus
perhaps backup old work files.
  cd $HOME
  tar c(z)f dbplussrc(platformname)(date).tar.gz dbplus
backup makefile and mysql config files
  cd $HOME/dbplus
  find . -name '?akefile' -print  | xargs tar czf make(platform).tar.gz 
  cp -p mysql-4.0.24/include/my_config.h my_config_(platform).h
  find . -name 'config.status' -print|xargs tar c(z)f config_status_(platform).tar(.gz)
update source 
  cd $HOME/ttt
  tar czf newsrc.tar.gz dbplus
  cd $HOME
  tar xzf ttt/newsrc.tar.gz
  rm -rf ttt
link files :
  cd $HOME/dbplus/inc
  rm -rf mysql
  ln -s ../mysql-4.0.24/include mysql
  cd ..
  cp -p my_config_(platform).h mysql-4.0.24/include 
  tar xzf make(platform).tar.gz
 
use this command to get a more files patch:
cd $DBPLUS_HOME
tar czf patch1201.tar.gz bin lib libexec --exclude lib/oci   
