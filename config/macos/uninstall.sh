#!/bin/bash
#
# plaso uninstallation script for macOS.
#
# This file is generated by l2tdevtools update-dependencies.py, any dependency
# related changes should be made in dependencies.ini.

EXIT_SUCCESS=0;
EXIT_FAILURE=1;

DEPENDENCIES="PyYAML XlsxWriter artifacts backports.lzma bencode biplist certifi chardet construct dateutil dfdatetime dfvfs dfwinreg dpkt dtfabric efilter elasticsearch-py future hachoir-core hachoir-metadata hachoir-parser idna libbde libesedb libevt libevtx libewf libfsntfs libfvde libfwnt libfwsi liblnk libmsiecf libolecf libqcow libregf libscca libsigscan libsmdev libsmraw libvhdi libvmdk libvshadow libvslvm lz4 pefile psutil pycrypto pyparsing pysqlite pytsk3 pytz pyzmq requests six urllib3 yara-python";

SCRIPT_NAME=`basename $0`;
DEPENDENCIES_ONLY=0;
SHOW_HELP=0;

echo "===============================================================";
echo " plaso macOS uninstallation script";
echo "===============================================================";

while test $# -gt 0;
do
  case $1 in
  -h | --help )
    SHOW_HELP=1;
    shift;
    ;;

  *)
    ;;
  esac
done

if test ${SHOW_HELP} -ne 0;
then
  echo "Usage: ./${SCRIPT_NAME} [--help]";
  echo "";
  echo "  --help: shows this help.";
  echo "";
  echo "";

  exit ${EXIT_SUCCESS};
fi

if test "$USER" != "root";
then
  echo "This script requires root privileges. Running: sudo.";

  sudo echo > /dev/null;
  if test $? -ne 0;
  then
    echo "Do you have root privileges?";

    exit ${EXIT_FAILURE};
  fi
fi

PACKAGE_IDENTIFIER=`/usr/sbin/pkgutil --packages | grep plaso`;

if test $? -eq 0;
then
  echo "Uninstalling plaso.";

  INSTALLED_FILES=`/usr/sbin/pkgutil --files ${PACKAGE_IDENTIFIER} --only-files`;

  for PATH in ${INSTALLED_FILES};
  do
    if test -f ${PATH};
    then
      rm -f ${PATH};
    fi
  done

  INSTALLED_FILES=`/usr/sbin/pkgutil --files ${PACKAGE_IDENTIFIER} --only-dirs | sort -r`;

  for PATH in ${INSTALLED_FILES};
  do
    if test -d ${PATH};
    then
      rmdir ${PATH} 2> /dev/null;
    fi
  done

  /usr/sbin/pkgutil --forget ${PACKAGE_IDENTIFIER};
fi

echo "Uninstalling dependencies.";

for PACKAGE_NAME in ${DEPENDENCIES};
do
  PACKAGE_IDENTIFIER=`/usr/sbin/pkgutil --packages | grep ${PACKAGE_NAME}`;

  if test $? -ne 0;
  then
    continue
  fi

  INSTALLED_FILES=`/usr/sbin/pkgutil --files ${PACKAGE_IDENTIFIER} --only-files`;

  for PATH in ${INSTALLED_FILES};
  do
    if test -f ${PATH};
    then
      rm -f ${PATH};
    fi
  done

  INSTALLED_FILES=`/usr/sbin/pkgutil --files ${PACKAGE_IDENTIFIER} --only-dirs | sort -r`;

  for PATH in ${INSTALLED_FILES};
  do
    if test -d ${PATH};
    then
      rmdir ${PATH} 2> /dev/null;
    fi
  done

  /usr/sbin/pkgutil --forget ${PACKAGE_IDENTIFIER};
done

echo "Done.";

exit ${EXIT_SUCCESS};

