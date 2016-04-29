import os.path
import sys
import datetime
import argparse

import numpy as np
import netCDF4

import json

FLOATSIZE = 4
MAXMEM = 512*1000*1024


class gradsDataset(object):
	
	def __init__(self, filename, global_attributes, lookups):
		
		lines = open(filename, 'r').readlines()
				
		self.attributes = global_attributes
		self.variables = {}
		
		self.calendar = 'standard'
		self.title = None
		self._varlist = []
		invarlist = False
		
		for line in lines:
			
			words = line.split()
			print len(words), words[0].lower()

			if len(words) == 0:
				continue
			
			if words[0].lower() == 'title':
				title = ''
				for word in words[1:]:
					title += word + ' '
			
			if words[0].lower() == 'dset':
				if words[1][0] == '^':
					self.dset = os.path.split(filename)[0] + './' + words[1][1:]
				else:
					self.dset = words[1]
					
			if words[0].lower() == 'undef':
				self.undef = float(words[1])
					
			if words[0].lower() == 'options':
				for word in words[1:]:
					if word == '365_day_calendar':
						self.calendar = '365_day'

			if words[0].lower() == 'xdef':
				if words[2] == 'linear':
					self.xsize = int(words[1])
					self.lon0 = float(words[3])
					self.dlon = float(words[4])
					
			if words[0].lower() == 'ydef':
				if words[2] == 'linear':
					self.ysize = int(words[1])
					self.lat0 = float(words[3])
					self.dlat = float(words[4])
					
			if words[0].lower() == 'zdef':
				if words[2] == 'linear':
					self.zsize = int(words[1])
					if self.zsize == 0:
						self.zsize = 1
					self.level0 = float(words[3])
					self.dlevel = float(words[4])

			if words[0].lower() == 'tdef':
				if words[2].lower() == 'linear':
					print 'linear', int(words[1])
					self.tsize = int(words[1])
					self.startdate = datetime.datetime.strptime(words[3], '%HZ%d%b%Y')
					self.timedelta, self.timeunits = self.parsetimedelta(words[4].lower())
					self.tunits = '%s since %s' % (self.timeunits, self.startdate.isoformat(' '))
					
			if words[0].lower() == 'vars':
				self.varcount = int(words[1])
				invarlist = True
				continue
				
			if words[0].lower() == 'endvars':
				invarlist = False
				continue
				
			if invarlist:
				description = ""
				for word in words[3:]:
					description += word + ' '
									
				if int(words[1]) == 0 or int(words[1]) == 1:
						
					variable = gradsVariable(words[0].lower(), self, ['time', 'lat', 'lon'], {})

					try:
						standard_name = lookups['variables'][words[0].lower()]['standard_name']
					except:
						standard_name = description
						
					try:
						units = lookups['variables'][words[0].lower()]['units']
					except:
						units = None
					
					try:
						long_name = lookups['variables'][words[0].lower()]['long_name']
					except:
						long_name = None
						
					variable.attributes['standard_name'] = standard_name
					if units:
						variable.attributes['units'] = units
					if long_name:
						variable.attributes['long_name'] = long_name
					
					self._varlist.append(variable.name)
					self.variables[words[0].lower()] = variable
				else:
					self._varlist.append(variable.name)
					self.variables[words[0].lower()] = variable
					
				print variable.name
					
		
		self.dfile = open(self.dset)

		if not self.attributes.has_key('title') and self.title:
			self.attributes['title'] = self.title
			
		self.attributes['history'] = '%s: grads2netcdf.py %s\n' % (datetime.datetime.now().isoformat(' '), os.path.split(filename)[1]) 
		
		try:
			for key in lookups['dataset']:
				self.attributes[key] = lookups['dataset'][key]
		except:
			pass
	
		self.dimensions = [('time', self.tsize), ('level', self.zsize), ('lat', self.ysize), ('lon', self.xsize)]
		
		self.variables['time'] = gradsVariable('time', self, ['time'], {'long_name':'time', 'units':self.tunits, 'calendar':self.calendar})
		self.variables['lat'] = gradsVariable('lat', self, ['lat'], {'long_name':'latitude', 'standard_name':'latitude', 'units': 'degrees_north'})
		self.variables['lon'] = gradsVariable('lon', self, ['lon'], {'long_name':'longitude', 'standard_name':'longitude', 'units': 'degrees_east'})
		self.variables['level'] = gradsVariable('level', self, ['level'], {'units':'meters', 'positive':'up'})
		
	def latitudes(self):		
		return np.array(range(0,self.ysize))*self.dlat + self.lat0
	
	def longitudes(self):
		return np.array(range(0,self.xsize))*self.dlon + self.lon0
		
	def levels(self):
		return np.array(range(0,self.zsize))*self.dlevel + self.level0


	def parsetimedelta(self, deltastring):
		
		if deltastring[-2:] == 'dy':
			return int(deltastring[:-2]), 'days'
			
		if deltastring[-2:] == 'mo':
			return int(deltastring[:-2]), 'months'

		
class gradsVariable(object):
	
	def __init__(self, name, dataset, dimensions, attributes={}):
	
		self.dimensions = dimensions
		self.attributes = attributes
		self.dataset = dataset
		self.name = name
		
	def __getitem__(self, slice):

		if self.name == 'time':
			return np.array(range(0,self.dataset.tsize))[slice]
			
		if self.name == 'level':
			return np.array(self.dataset.levels())[slice]
		
		if self.name == 'lat':
			return np.array(self.dataset.latitudes())[slice]

		if self.name == 'lon':
			return np.array(self.dataset.longitudes())[slice]

		try:
			tstart, tend, tstep = slice[0].indices(self.dataset.tsize)
		except:
			tstart = slice[0]
			tend = tstart+1
	
		fieldsize = self.dataset.xsize * self.dataset.ysize
		
		var_index = self.dataset._varlist.index(self.name)
			
		self.dataset.dfile.seek(len(self.dataset._varlist)* fieldsize * tstart + fieldsize*var_index)
		#self.dataset.dfile.seek(fieldsize * tstart)
		result = np.fromfile(self.dataset.dfile, dtype=np.float32, count=fieldsize * (tend - tstart))
		
		return result.reshape(((tend - tstart), self.dataset.ysize, self.dataset.xsize))
				

# Process arguments
parser = argparse.ArgumentParser()
parser.add_argument('-o', dest='outfile')
parser.add_argument('infile')
parser.add_argument('attributes', nargs='*')
args = parser.parse_args()

try:
	lookups = json.load(open('grads2netcdf.json'))
except:
	lookups = {}
	
global_attributes = {}
for arg in sys.argv[1:]:

	splitarg = arg.split(':')	
	if len(splitarg) == 2:
		global_attributes[splitarg[0]] = splitarg[1]


source = gradsDataset(args.infile, global_attributes, lookups)

if args.outfile:
	ncfilename = args.outfile
else:
	ncfilename = os.path.split(args.infile)[1][:-4] + '.nc'
		
ncfile = netCDF4.Dataset(ncfilename, 'w', format='NETCDF3_CLASSIC')
ncfile.set_fill_off()

for key,value in source.attributes.iteritems():
	ncfile.setncattr(key, value)

for dimension in source.dimensions:
	ncfile.createDimension(dimension[0], dimension[1])
	
for varname in source.variables:
	ncfile.createVariable(varname, 'f4', tuple(source.variables[varname].dimensions), fill_value=source.undef)
	for key,value in source.variables[varname].attributes.iteritems():
		ncfile.variables[varname].setncattr(key, value)

ncfile.variables['time'][:] = source.variables['time'][:]
ncfile.variables['level'][:] = source.variables['level'][:]
ncfile.variables['lat'][:] = source.variables['lat'][:]
ncfile.variables['lon'][:] = source.variables['lon'][:]

chunksize = int((MAXMEM / 4) / (source.xsize * source.ysize))

#print source.variables['pr'][0,:].shape
#for t in range(0,source.tsize,chunksize):
#	if t+chunksize >= source.tsize:
#		chunksize -= t+chunksize - source.tsize
#	for var in source._varlist:
#		ncfile.variables[var][t:t+chunksize,:] = source.variables[var][t:t+chunksize,:]

for var in source._varlist:
        ncfile.variables[var][:,:] = source.variables[var][:,:]

	
ncfile.close()





