require "log"
require "spec"

require "../src/dream"

struct Slice(T)
  def pretty_print(pp : PrettyPrint)
    pp.text "Bytes[#{self.hexstring}]"
  end
end

alias Config = {index: Dream::Index, seed: Int32}
config = Config.from_yaml File.read ENV["SPEC_CONFIG_PATH"]
rnd = Random.new config[:seed]
index = config[:index]

describe Dream do
  describe "Index" do
    it "simple test", focus: true do
      a = "a".to_slice
      b = "b".to_slice
      c = "c".to_slice
      o1 = "o1".to_slice
      o2 = "o2".to_slice
      o3 = "o3".to_slice

      index.transaction.add(o1, [a]).commit
      index.transaction.add(o2, [a, b]).commit
      index.transaction.add(o3, [a, b, c]).commit

      index.find([a, b, c], limit: 2).map { |i| index[i]?.not_nil! }.should eq [o3]

      index.find([a, b]).map { |i| index[i]?.not_nil! }.should eq [o2, o3]
      index.find([a, b], limit: 1).map { |i| index[i]?.not_nil! }.should eq [o2]
      index.find([a]).map { |i| index[i]?.not_nil! }.sort.should eq [o1, o2, o3].sort
      index.find([a], limit: 2).map { |i| index[i]?.not_nil! }.sort.should eq [o1, o2].sort
      index.find([a], limit: 1).map { |i| index[i]?.not_nil! }.should eq [o2]

      index.find([a], [a]).map { |i| index[i]?.not_nil! }.should eq [] of Bytes
      index.find([a], [b]).map { |i| index[i]?.not_nil! }.should eq [o1]
      index.find([a], [c]).map { |i| index[i]?.not_nil! }.sort.should eq [o1, o2].sort
      index.find([b], [a]).map { |i| index[i]?.not_nil! }.should eq [] of Bytes
      index.find([b], [c]).map { |i| index[i]?.not_nil! }.should eq [o2]
      index.find([a, b], [c]).map { |i| index[i]?.not_nil! }.should eq [o2]

      index.transaction.delete(o3, [a, c]).commit
      index.find([a]).map { |i| index[i]?.not_nil! }.sort.should eq [o1, o2].sort
      index.find([b]).map { |i| index[i]?.not_nil! }.should eq [o2, o3]
      index.find([c]).map { |i| index[i]?.not_nil! }.should eq [] of Bytes

      index.transaction.delete(o2).commit
      index.find([a]).map { |i| index[i]?.not_nil! }.should eq [o1]
      index.find([b]).map { |i| index[i]?.not_nil! }.should eq [o3]
      index.find([c]).map { |i| index[i]?.not_nil! }.should eq [] of Bytes

      index.transaction.delete(o1).commit
      index.transaction.delete(o3).commit
    end
    # it "generative test" do
    #   rnd = Random.new 0

    #   tags_count = 8
    #   objects_count = 100
    #   tags_per_object_count = 3
    #   searches_count = 10

    #   tags = (1..tags_count).map { rnd.random_bytes 16 }
    #   objects = Hash.zip (1..objects_count).map { rnd.random_bytes 16 }, (1..objects_count).map { (tags.sample tags_per_object_count, rnd).sort }
    #   t2o = {} of Bytes => Set(Bytes)
    #   objects.each { |o, tt| tt.each do |t|
    #     t2o[t] = Set(Bytes).new unless t2o[t]?
    #     t2o[t] << o
    #   end }
    #   searches = (1..searches_count).map { tags.sample 2, rnd }

    #   objects.each { |oid, tags| index.add oid, tags }
    #   objects.each { |o, tt| tt.each { |t| (index.has_tag? o, t).should eq true } }

    #   objects.each { |oid, tags| (index.get oid).map { |i| index[i]?.not_nil! }.sort.should eq tags }
    #   searches.each do |tags|
    #     r = index.find(tags).map { |i| index[i]?.not_nil! }.sort
    #     correct = tags.map { |t| t2o[t] }.reduce { |acc, cur| acc &= cur }.to_a.sort
    #     r.should eq correct

    #     r = [] of Dream::Id
    #     until (rp = index.find(tags, limit: 2_u64, from: (r.last rescue nil))).empty?
    #       r += rp
    #     end
    #     r.map { |i| index[i]?.not_nil! }.sort.should eq correct
    #   end
    # end
  end
end
