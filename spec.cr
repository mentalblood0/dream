require "spec"

require "./src/dream.cr"

describe Sophia do
  describe "Index" do
    ind = Dream::Index.new "/tmp/dream"
    ind.add "o1", ["a", "b", "c"]
    ind.add "o2", ["a", "b"]
    ind.add "o3", ["a"]
    ind.find(["a", "b", "c"]).should eq Set.new ["o1"]
    ind.find(["a", "b"]).should eq Set.new ["o1", "o2"]
    ind.find(["a"]).should eq Set.new ["o1", "o2", "o3"]
  end
end
