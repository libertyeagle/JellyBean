------------------------------------------------------------------------
--[[ GRUST ]]--
-- Author: Remi Cadene

-- Gated Recurrent Units architecture for Skip Thoughts vectors
--
-- For p > 0, it becomes Bayesian GRUSTs [Moon et al., 2015; Gal, 2015].
-- In this case, please do not dropout on input as BGRUSTs handle the input with 
-- its own dropouts. First, try 0.25 for p as Gal (2016) suggested, presumably, 
-- because of summations of two parts in GRUSTs connections. 
------------------------------------------------------------------------
require 'skipthoughts.MaskZeroCopy'

local GRUST, parent = torch.class('nn.GRUST', 'nn.AbstractRecurrent')

function GRUST:__init(inputSize, outputSize, rho, p, mono)
   parent.__init(self, rho or 9999)
   self.p = p or 0
   if p and p ~= 0 then
      assert(nn.Dropout(p,false,false,true).lazy, 'only work with Lazy Dropout!')
   end
   self.mono = mono or false
   self.inputSize = inputSize
   self.outputSize = outputSize   
   -- build the model
   self.recurrentModule = self:buildModel()
   -- make it work with nn.Container
   self.modules[1] = self.recurrentModule
   self.sharedClones[1] = self.recurrentModule 
   
   -- for output(0), cell(0) and gradCell(T)
   self.zeroTensor = torch.Tensor() 
   
   self.cells = {}
   self.gradCells = {}
end

function GRUST:buildModel()
   -- input : {input, prevOutput}
   -- output : {output}
   
   -- Calculate all four gates in one go : input, hidden, forget, output
   if self.p ~= 0 then
      self.i2g = nn.Sequential()
                     :add(nn.ConcatTable()
                        :add(nn.Dropout(self.p,false,false,true,self.mono))
                        :add(nn.Dropout(self.p,false,false,true,self.mono)))
                     :add(nn.ParallelTable()
                        :add(nn.Linear(self.inputSize, self.outputSize))
                        :add(nn.Linear(self.inputSize, self.outputSize)))
                     :add(nn.JoinTable(2))
      self.o2g = nn.Sequential()
                     :add(nn.ConcatTable()
                        :add(nn.Dropout(self.p,false,false,true,self.mono))
                        :add(nn.Dropout(self.p,false,false,true,self.mono)))
                     :add(nn.ParallelTable()
                        :add(nn.LinearNoBias(self.outputSize, self.outputSize))
                        :add(nn.LinearNoBias(self.outputSize, self.outputSize)))
                     :add(nn.JoinTable(2))
   else
      self.i2g = nn.Linear(self.inputSize, 2*self.outputSize)
      self.o2g = nn.LinearNoBias(self.outputSize, 2*self.outputSize)
   end

   local para = nn.ParallelTable():add(self.i2g):add(self.o2g)
   local gates = nn.Sequential()
   gates:add(para)
   gates:add(nn.CAddTable())

   -- Reshape to (batch_size, n_gates, hid_size)
   -- Then slize the n_gates dimension, i.e dimension 2
   gates:add(nn.Reshape(2,self.outputSize))
   gates:add(nn.SplitTable(1,2))
   local transfer = nn.ParallelTable()
   transfer:add(nn.Sigmoid()):add(nn.Sigmoid())
   gates:add(transfer)

   local concat = nn.ConcatTable():add(nn.Identity()):add(gates)
   local seq = nn.Sequential()
   seq:add(concat)
   seq:add(nn.FlattenTable()) -- x(t), s(t-1), r, z

   -- Rearrange to x(t), s(t-1), r, z, s(t-1)
   local concat = nn.ConcatTable()  -- 
   concat:add(nn.NarrowTable(1,4)):add(nn.SelectTable(2))
   seq:add(concat):add(nn.FlattenTable())

   -- h
   local hidden = nn.Sequential()
   local concat = nn.ConcatTable()
   -- t1
   local t1 = nn.Sequential()
   t1:add(nn.SelectTable(1)) -- x(t)
   if self.p ~= 0 then
      t1:add(nn.Dropout(self.p,false,false,true,self.mono))
   end
   t1:add(nn.Linear(self.inputSize, self.outputSize))
   -- t2
   local t2 = nn.Sequential()
   local concat2 = nn.ConcatTable()
   -- t21
   local t21 = nn.Sequential()
   t21:add(nn.SelectTable(2)) -- s(t-1)
   if self.p ~= 0 then
      t21:add(nn.Dropout(self.p,false,false,true,self.mono))
   end
   t21:add(nn.LinearNoBias(self.outputSize, self.outputSize))
   -- t22
   local t22 = nn.Sequential()
   t22:add(nn.SelectTable(3)) -- r 
   if self.p ~= 0 then
      t22:add(nn.Dropout(self.p,false,false,true,self.mono))
   end
   concat2:add(t21):add(t22)
   t2:add(concat2):add(nn.CMulTable())
   concat:add(t1):add(t2)
   hidden:add(concat):add(nn.CAddTable()):add(nn.Tanh())
   
   local z1 = nn.Sequential()
   z1:add(nn.SelectTable(4))
   z1:add(nn.SAdd(-1, true))  -- Scalar add & negation

   local z2 = nn.Sequential()
   z2:add(nn.NarrowTable(4,2)) -- z, s
   z2:add(nn.CMulTable())

   local o1 = nn.Sequential()
   local concat = nn.ConcatTable()
   concat:add(hidden):add(z1)
   o1:add(concat):add(nn.CMulTable())

   local o2 = nn.Sequential()
   local concat = nn.ConcatTable()
   concat:add(o1):add(z2)
   o2:add(concat):add(nn.CAddTable())

   seq:add(o2)
   
   return seq
end

------------------------- forward backward -----------------------------
function GRUST:updateOutput(input)
   local prevOutput
   if self.step == 1 then
      prevOutput = self.userPrevOutput or self.zeroTensor
      if input:dim() == 2 then
         self.zeroTensor:resize(input:size(1), self.outputSize):zero()
      else
         self.zeroTensor:resize(self.outputSize):zero()
      end
   else
      -- previous output and cell of this module
      prevOutput = self.outputs[self.step-1]
   end

   -- output(t) = GRUST{input(t), output(t-1)}
   local output
   if self.train ~= false then
      self:recycle()
      local recurrentModule = self:getStepModule(self.step)
      -- the actual forward propagation
      output = recurrentModule:updateOutput{input, prevOutput}
   else
      output = self.recurrentModule:updateOutput{input, prevOutput}
   end
   
   self.outputs[self.step] = output
   
   self.output = output
   
   self.step = self.step + 1
   self.gradPrevOutput = nil
   self.updateGradInputStep = nil
   self.accGradParametersStep = nil
   -- note that we don't return the cell, just the output
   return self.output
end

function GRUST:_updateGradInput(input, gradOutput)
   assert(self.step > 1, "expecting at least one updateOutput")
   local step = self.updateGradInputStep - 1
   assert(step >= 1)
   
   local gradInput
   -- set the output/gradOutput states of current Module
   local recurrentModule = self:getStepModule(step)
   
   -- backward propagate through this step
   if self.gradPrevOutput then
      self._gradOutputs[step] = nn.rnn.recursiveCopy(self._gradOutputs[step], self.gradPrevOutput)
      nn.rnn.recursiveAdd(self._gradOutputs[step], gradOutput)
      gradOutput = self._gradOutputs[step]
   end
   
   local output = (step == 1) and (self.userPrevOutput or self.zeroTensor) or self.outputs[step-1]
   local inputTable = {input, output}
   local gradInputTable = recurrentModule:updateGradInput(inputTable, gradOutput)
   gradInput, self.gradPrevOutput = unpack(gradInputTable)
   if self.userPrevOutput then self.userGradPrevOutput = self.gradPrevOutput end
   
   return gradInput
end

function GRUST:_accGradParameters(input, gradOutput, scale)
   local step = self.accGradParametersStep - 1
   assert(step >= 1)
   
   -- set the output/gradOutput states of current Module
   local recurrentModule = self:getStepModule(step)
   
   -- backward propagate through this step
   local output = (step == 1) and (self.userPrevOutput or self.zeroTensor) or self.outputs[step-1]
   local inputTable = {input, output}
   local gradOutput = (step == self.step-1) and gradOutput or self._gradOutputs[step]
   recurrentModule:accGradParameters(inputTable, gradOutput, scale)
   return gradInput
end

function GRUST:__tostring__()
   return string.format('%s(%d -> %d, %.2f)', torch.type(self), self.inputSize, self.outputSize, self.p)
end

-- migrate GRUSTs params to BGRUSTs params
function GRUST:migrate(params)
   local _params = self:parameters()
   assert(self.p ~= 0, 'only support for BGRUSTs.')
   assert(#params == 6, '# of source params should be 6.')
   assert(#_params == 9, '# of destination params should be 9.')
   _params[1]:copy(params[1]:narrow(1,1,self.outputSize))
   _params[2]:copy(params[2]:narrow(1,1,self.outputSize))
   _params[3]:copy(params[1]:narrow(1,self.outputSize+1,self.outputSize))
   _params[4]:copy(params[2]:narrow(1,self.outputSize+1,self.outputSize))
   _params[5]:copy(params[3]:narrow(1,1,self.outputSize))
   _params[6]:copy(params[3]:narrow(1,self.outputSize+1,self.outputSize))
   _params[7]:copy(params[4])
   _params[8]:copy(params[5])
   _params[9]:copy(params[6])
end

-- maskZeroCopy is usefull when your data are right-zero-padded
-- instead of the usual left-zero-padding.
-- When a zero is encountered, the lastOutput are copied.
-- maskZeroCopy is made for the backward RNN of a bi-GRU only.
function GRUST:maskZeroCopy(nInputDim, backward)
   self.recurrentModule = nn.MaskZeroCopy(self.recurrentModule, nInputDim, true, backward)
   self.sharedClones = {self.recurrentModule}
   self.modules[1] = self.recurrentModule
   return self
end

-- function GRUST:trimZeroST(nInputDim)
--    if torch.typename(self)=='nn.GRU' and self.p ~= 0 then
--       assert(self.mono, "TrimZero for BGRU needs `mono` option.")
--    end
--    self.recurrentModule = nn.TrimZero(self.recurrentModule, nInputDim, true)
--    self.sharedClones = {self.recurrentModule}
--    self.modules[1] = self.recurrentModule
--    return self
-- end