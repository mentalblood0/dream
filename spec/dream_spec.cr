require "log"
require "spec"

require "../src/dream"

struct Slice(T)
  def pretty_print(pp : PrettyPrint)
    pp.text "Bytes[#{self.hexstring}]"
  end
end

alias Config = {index: Dream::Index, seed: Int32, generative: {tags: Int32, objects: Int32, tags_per_object: Int32, searches: Int32}}
config = Config.from_yaml File.read ENV["SPEC_CONFIG_PATH"]
rnd = Random.new config[:seed]
index = config[:index]

Spec.before_each do
  index.clear
end

describe Dream do
  describe "Index" do
    it "simple test" do
      a = "a".to_slice
      b = "b".to_slice
      c = "c".to_slice
      o1 = "o1".to_slice
      o2 = "o2".to_slice
      o3 = "o3".to_slice

      transaction = index
        .transaction
        .add(o1, [a])
        .add(o2, [a, b])
        .add(o3, [a, b, c])

      transaction.find([a, b, c], limit: 2).map { |i| transaction[i]?.not_nil! }.should eq [o3]

      transaction.find([a, b]).map { |i| transaction[i]?.not_nil! }.should eq [o2, o3]
      transaction.find([a, b], limit: 1).map { |i| transaction[i]?.not_nil! }.should eq [o2]
      transaction.find([a]).map { |i| transaction[i]?.not_nil! }.sort.should eq [o1, o2, o3].sort
      transaction.find([a], limit: 2).map { |i| transaction[i]?.not_nil! }.sort.should eq [o1, o2].sort
      transaction.find([a], limit: 1).map { |i| transaction[i]?.not_nil! }.should eq [o2]

      transaction.find([a], [a]).map { |i| transaction[i]?.not_nil! }.should eq [] of Bytes
      transaction.find([a], [b]).map { |i| transaction[i]?.not_nil! }.should eq [o1]
      transaction.find([a], [c]).map { |i| transaction[i]?.not_nil! }.sort.should eq [o1, o2].sort
      transaction.find([b], [a]).map { |i| transaction[i]?.not_nil! }.should eq [] of Bytes
      transaction.find([b], [c]).map { |i| transaction[i]?.not_nil! }.should eq [o2]
      transaction.find([a, b], [c]).map { |i| transaction[i]?.not_nil! }.should eq [o2]

      transaction.delete o3, [a, c]
      transaction.find([a]).map { |i| transaction[i]?.not_nil! }.sort.should eq [o1, o2].sort
      transaction.find([b]).map { |i| transaction[i]?.not_nil! }.should eq [o2, o3]
      transaction.find([c]).map { |i| transaction[i]?.not_nil! }.should eq [] of Bytes

      transaction.delete o2
      transaction.find([a]).map { |i| transaction[i]?.not_nil! }.should eq [o1]
      transaction.find([b]).map { |i| transaction[i]?.not_nil! }.should eq [o3]
      transaction.find([c]).map { |i| transaction[i]?.not_nil! }.should eq [] of Bytes

      transaction.delete o1
      transaction.delete o3
    end
    it "generative test" do
      tags = (1..config[:generative][:tags]).map { rnd.random_bytes 16 }
      objects = Hash.zip (1..config[:generative][:objects]).map { rnd.random_bytes 16 }, (1..config[:generative][:objects]).map { (tags.sample config[:generative][:tags_per_object], rnd).sort }
      tags_to_objects = {} of Bytes => Set(Bytes)
      objects.each { |object, tags| tags.each do |tag|
        tags_to_objects[tag] = Set(Bytes).new unless tags_to_objects[tag]?
        tags_to_objects[tag] << object
      end }
      searches = (1..config[:generative][:searches]).map { tags.sample 2, rnd }

      transaction = index.transaction
      objects.each { |object, tags| transaction.add object, tags }
      objects.each { |object, tags| tags.each { |tag| (transaction.has_tag? object, tag).should eq true } }

      objects.each { |object, tags| (transaction.get object).map { |tag_id| transaction[tag_id]?.not_nil! }.sort.should eq tags }
      searches.each do |tags|
        result = transaction.find(tags).map { |i| transaction[i]?.not_nil! }.sort
        correct = tags.map { |tag| tags_to_objects[tag] }.reduce { |acc, cur| acc &= cur }.to_a.sort
        result.should eq correct

        result = [] of Dream::Id
        until (result_part = transaction.find(present_tags: tags, limit: 2_u64, start_after_object: (result.last rescue nil))).empty?
          result += result_part
          (result.size <= correct.size).should eq true
        end
        result.map { |tag_id| transaction[tag_id]?.not_nil! }.sort.should eq correct
      end
    end
  end
end
