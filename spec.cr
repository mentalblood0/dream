require "spec"

require "./src/dream.cr"

describe Sophia do
  describe "Index" do
    ind = Dream::Index.new "/tmp/dream"
    ind.add "o1", ["a"]
    ind.add "o2", ["a", "b"]
    ind.add "o3", ["a", "b", "c"]
    ind.find(["a", "b", "c"], 2).should eq ["o3"]
    ind.find(["a", "b"]).should eq ["o2", "o3"]
    ind.find(["a", "b"], 1).should eq ["o2"]
    ind.find(["a"]).should eq ["o1", "o2", "o3"]
    ind.find(["a"], 2).should eq ["o1", "o2"]
    ind.find(["a"], 1).should eq ["o1"]
  end
end
