local ffi = require("ffi")

ffi.cdef [[
struct {
    double cdf_;
    double val_;
} CDFentry;;
]]

local fsdMod = {
   ["minCDF_"] = 0,
   ["maxCDF_"] = 0,
   ["maxEntry_"] = 32,
   ["table_"] = 0,
   ["numEntry_"] = 0,
   ["INTER_DISCRETE"] = 0, -- no interpolation
   ["INTER_CONTINUOUS"] = 1, -- linear
   ["INTER_INTEGRAL"] = 2, -- linear and round up
}

function fsdMod:new (o)
   o = o or {}
   setmetatable(o, self)
   self.__index = self
   return o
end

function fsdMod:loadCDF(filename)
   local numLines = 0
   for line in io.lines(filename) do
      numLines = numLines + 1
   end         
   self.table_ = ffi.new("CDFentry[?]", numLines)
   local lineNum = 0
   for line in io.lines(filename) do
      local val, index, cdf = line:match("(%d+) (%d+) (%d+)")
      assert(val ~= nil)
      assert(index ~= nil)
      assert(cdf ~= nil)
      table_[lineNum].val = val
      table_[lineNum].cdf = cdf
      lineNum = lineNum + 1
   end

   self.numEntry_ = lineNum
   return lineNum
end

function fsdMod:avg()
   local avg = 0
   for i = 0, self.numEntry-1 do
      local value = 0
      local prob = 0
      if i == 0 then
	 value = self.table_[0].val/2
	 prob = self.table_[0].cdf_
      else
	 value = (self.table_[i-1].val + self.table_[i].val)/2
	 prob = self.table_[i].cdf_ - self.table_[i-1].cdf_	 
      end
      avg = avg + value * prob
   end
   return avg
end

function fsdMod:value()
   if self.numEntry_ <= 0 then return 0 end
   local u = math.random(self.minCDF_, self.maxCDF_)
   local mid = self:lookup(u)
   if (mid > 0 and self.interpolation_ > 0 and u < self.table_[mid].cdf_) then
      return self.interpolate(u, self.table_[mid-1].cdf, self.table_[mid-1].val_,
				self.table_[mid].cdf_, self.table_[mid].val_)
   end
end

function fsdMod:interpolate(x, x1, y1, x2, y2)
   value = y1 + ((x-x1) * ((y2-y1) / (x2-x1)))
   if (self.interpolation_ == self.INTER_INTEGRAL) then
      return math.ceil(value)
   end
   return value
end

function fsdMod:lookup(u)
   local lo = 1
   local hi = fsMod.numEntry_-1
   local mid = nil
   if (u <= self.table_[0].cdf_) then return 0 end
   while lo < hi do
      mid = (lo + hi) / 2
      if (u > self.table_[mid].cdf_) then lo = mid + 1
      else hi = mid end
   end
   return lo
end


