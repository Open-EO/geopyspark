#!/bin/sh

set -e

GDALOPTS="  --with-geos \
            --with-expat \
            --with-curl \
            --with-python \
            --with-java \
            --with-libz=internal \
            --with-libtiff=internal \
            --with-geotiff=internal \
            --with-proj \
            --without-libtool \
            --without-gif \
            --without-pg \
            --without-grass \
            --without-libgrass \
            --without-cfitsio \
            --without-pcraster \
            --without-netcdf \
            --without-png \
            --without-jpeg \
            --without-gif \
            --without-ogdi \
            --without-fme \
            --without-hdf4 \
            --without-hdf5 \
            --without-jasper \
            --without-ecw \
            --without-kakadu \
            --without-mrsid \
            --without-jp2mrsid \
            --without-bsb \
            --without-grib \
            --without-mysql \
            --without-ingres \
            --without-xerces \
            --without-odbc \
            --without-sqlite3 \
            --without-idb \
            --without-sde \
            --without-perl"

# Create build dir if not exists
if [ ! -d "$GDALBUILD" ]; then
  mkdir $GDALBUILD;
fi

if [ ! -d "$GDALINST" ]; then
  mkdir $GDALINST;
fi

ls -l $GDALINST

if [ ! -d "$GDALINST/gdal-$GDAL_VERSION" ]; then
  cd $GDALBUILD
  wget http://download.osgeo.org/gdal/$GDAL_VERSION/gdal-$GDAL_VERSION.tar.gz
  tar -xzf gdal-$GDAL_VERSION.tar.gz
  cd gdal-$GDAL_VERSION
  ./configure --prefix=$GDALINST/gdal-$GDAL_VERSION $GDALOPTS
  make -j 2
  make install
  cd swig/java && make && make install
  cd $GDALBUILD/gdal-${GDAL_VERSION}/swig/python \
    && python3 setup.py build \
    && python3 setup.py install
fi

# change back to travis build dir
cd $TRAVIS_BUILD_DIR