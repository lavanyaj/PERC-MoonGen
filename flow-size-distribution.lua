local log = require "log"
local ffi = require("ffi")

ffi.cdef [[
typedef struct {
    double cdf_;
    double val_;
} CDFentry;
]]

local fsdMod = {
   ["minCDF_"] = 0.0,
   ["maxCDF_"] = 1.0,
   ["maxEntry_"] = 32,
   ["table_"] = 0,
   ["numEntry_"] = 0,
   ["interpolation_"] = 1,
   ["INTER_DISCRETE"] = 0, -- no interpolation
   ["INTER_CONTINUOUS"] = 1, -- linear
   ["INTER_INTEGRAL"] = 2, -- linear and round up
}

fsdMod.__index = fsdMod

function fsdMod.create ()
   local fsd = {}
   setmetatable(fsd, fsdMod)
   return fsd
end

function fsdMod:loadCDF(filename)
   math.randomseed(os.time())
   local numLines = 0
   for line in io.lines(filename) do
      numLines = numLines + 1
   end         
   self.table_ = ffi.new("CDFentry[?]", numLines)
   local lineNum = 0
   for line in io.lines(filename) do      
      local val, index, cdf = line:match("(.+)%s+(.+)%s+(.+)")
      -- print(line)
      -- print(val .. ", " .. index .. ", " .. cdf)
      assert(val ~= nil)
      assert(index ~= nil)
      assert(cdf ~= nil)
      self.table_[lineNum].val_ = tonumber(val)
      self.table_[lineNum].cdf_ = tonumber(cdf)
      assert(self.table_[lineNum].val_ ~= nil)
      assert(self.table_[lineNum].cdf_ ~= nil)
      lineNum = lineNum + 1
   end

   self.numEntry_ = lineNum
   return lineNum
end

function fsdMod:avg()
   local avg = 0
   for i = 0, self.numEntry_-1 do
      local value = 0
      local prob = 0
      if i == 0 then
	 value = self.table_[0].val_/2
	 prob = self.table_[0].cdf_
      else
	 value = (self.table_[i-1].val_ + self.table_[i].val_)/2
	 prob = self.table_[i].cdf_ - self.table_[i-1].cdf_	 
      end
      avg = avg + value * prob
   end
   return avg
end

function fsdMod:value()
   if self.numEntry_ <= 0 then
      -- print("0 entries, returning 0")
      return 0
   end
   local u = math.random(self.minCDF_, self.maxCDF_*1000)/1000
   -- print("dart at " .. u)
   local mid = self:lookup(u)
   -- print("lookup(".. u .. ") returned " .. mid)
   -- print("mid: " .. mid .. ", self.interpolation_: "
	    -- .. tostring(self.interpolation_)
	    -- .. ", self.table_[".. tostring(mid) .. "].cdf_: "
	    -- .. tostring(self.table_[mid].cdf_))
   assert(mid ~= nil)
   if (mid > 0
	  and self.interpolation_ > 0
       and u <= self.table_[mid].cdf_) then
      -- print(self.table_[mid-1].cdf_)
      -- print(self.table_[mid-1].val_)
      -- print(self.table_[mid].cdf_)
      -- print(self.table_[mid].val_)
      
      return self:interpolate(u, self.table_[mid-1].cdf_, self.table_[mid-1].val_,
				self.table_[mid].cdf_, self.table_[mid].val_)
   end
   log:warn("didn't find a flow size at probability " .. tostring(u))
   return self:value()
end

function fsdMod:interpolate(x, x1, y1, x2, y2)
   -- print(x)
   -- print(x1)
   -- print(y1)
   -- print(x2)
   -- print(y2)
   local value = y1 + ((x-x1) * ((y2-y1) / (x2-x1)))
   -- print("interpolating from u " .. x
	--    .. " given cdf " .. x1 .. ", val " .. y1
	--    .. " and cdf " .. x2 .. ", val " .. y2
	--    .. ": " .. value)
   if (self.interpolation_ == self.INTER_INTEGRAL) then
      return math.ceil(value)
   end
   return value
end

function fsdMod:lookup(u)
   -- print("Looking up " .. u)
   local lo = 1
   local hi = self.numEntry_-1
   -- print("lo " .. lo .. ", hi " .. hi)
   local mid = nil
   if (u <= self.table_[0].cdf_) then
      -- print("u <= self.table_[0].cdf_ " .. tostring(self.table_[0].cdf_))
      -- print("so return 0")
      return 0
   end
   local i = 1
   while lo < hi do
      mid = (lo + hi) / 2
      -- print("lookup " .. i .. ": lo " .. lo .. ", hi " .. hi .. ", mid " .. mid)
      -- print("self.table_[".. mid .. "].cdf_: " .. tostring(self.table_[mid].cdf_))
      if (u > self.table_[mid].cdf_) then
	 lo = mid + 1
	 -- print("lo: mid + 1 i.e., " .. lo)
      else
	 hi = mid
	 -- print("hi: mid i.e., " .. hi)
      end
      i = i + 1
   end
   -- print("returning lo " .. lo)
   return lo
end

return fsdMod

--local f = fsdMod.create()
--f:loadCDF("DCTCP_CDF")
---- print(f:avg())
