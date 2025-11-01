require "log"
require "yaml"

require "lawn/Database"
require "lawn/Transaction"
require "xxhash128"

module Dream
  def self.digest(source : Bytes)
    digest = LibXxhash.xxhash128 source, source.size, 0
    result = Bytes.new 16
    IO::ByteFormat::BigEndian.encode digest.high64, result[0..7]
    IO::ByteFormat::BigEndian.encode digest.low64, result[8..15]
    result
  end

  record Id, value : Bytes

  TAG_AND_OBJECT       = 0_u8
  OBJECT_AND_TAG       = 1_u8
  IDS_TO_SOURCES       = 2_u8
  TAG_TO_OBJECTS_COUNT = 3_u8
  OBJECT_TO_TAGS_COUNT = 4_u8

  class Transaction
    getter transaction : Lawn::Transaction

    def initialize(@transaction)
    end

    protected def number_from_bytes(bytes : Bytes)
      IO::ByteFormat::BigEndian.decode UInt32, bytes
    end

    protected def number_to_bytes(number : UInt32)
      result = Bytes.new 4
      IO::ByteFormat::BigEndian.encode number, result
      result
    end

    def add(object : Bytes | Id, tags : Array(Bytes | Id))
      Log.debug { "#{self.class}.add object: #{object.is_a?(Bytes) ? object.hexstring : object.value.hexstring}, tags: #{tags.map { |tag| tag.is_a?(Bytes) ? tag.hexstring : tag.value.hexstring }}" }
      object_id = object.is_a?(Bytes) ? Dream.digest(object) : object.value
      @transaction.set IDS_TO_SOURCES, object_id, object if object.is_a? Bytes
      tags.each do |tag|
        tag_id = tag.is_a?(Bytes) ? Dream.digest(tag) : tag.value
        @transaction.set TAG_AND_OBJECT, tag_id + object_id
        @transaction.set OBJECT_AND_TAG, object_id + tag_id
        @transaction.set IDS_TO_SOURCES, tag_id, tag if tag.is_a? Bytes
        @transaction.set TAG_TO_OBJECTS_COUNT, tag_id, number_to_bytes 1_u32 + ((current_count = @transaction.get(TAG_TO_OBJECTS_COUNT, tag_id)) ? number_from_bytes(current_count) : 0_u32)
      end
      @transaction.set OBJECT_TO_TAGS_COUNT, object_id, number_to_bytes tags.size.to_u32 + ((current_count = @transaction.get(OBJECT_TO_TAGS_COUNT, object_id)) ? number_from_bytes(current_count) : 0_u32)
      self
    end

    def delete(object : Bytes | Id)
      Log.debug { "#{self.class}.delete #{object.is_a?(Bytes) ? object.hexstring : object.value.hexstring}" }
      object_id = object.is_a?(Bytes) ? Dream.digest(object) : object.value
      return unless @transaction.get OBJECT_TO_TAGS_COUNT, object_id
      @transaction.delete IDS_TO_SOURCES, object_id if object.is_a? Bytes
      @transaction.cursor(OBJECT_AND_TAG, from: object_id).each_next do |current_object_to_tag, _|
        current_object_id = current_object_to_tag[..15]
        break unless current_object_id == object_id
        current_tag_id = current_object_to_tag[16..]
        @transaction.delete TAG_AND_OBJECT, current_tag_id + current_object_id
        @transaction.delete OBJECT_AND_TAG, current_object_to_tag
        @transaction.set TAG_TO_OBJECTS_COUNT, current_tag_id, number_to_bytes number_from_bytes(@transaction.get(TAG_TO_OBJECTS_COUNT, current_tag_id).not_nil!) - 1
      end
      @transaction.delete OBJECT_TO_TAGS_COUNT, object_id
      self
    end

    def delete(object : Bytes | Id, tags : Array(Bytes | Id))
      Log.debug { "#{self.class}.delete object: #{object.is_a?(Bytes) ? object.hexstring : object.value.hexstring}, tags: #{tags.map { |tag| tag.is_a?(Bytes) ? tag.hexstring : tag.value.hexstring }}" }
      object_id = object.is_a?(Bytes) ? Dream.digest(object) : object.value
      return unless @transaction.get OBJECT_TO_TAGS_COUNT, object_id
      tags_removed_from_object = 0
      tags.each do |tag|
        tag_id = tag.is_a?(Bytes) ? Dream.digest(tag) : tag.value
        next unless @transaction.get TAG_AND_OBJECT, tag_id + object_id
        @transaction.delete TAG_AND_OBJECT, tag_id + object_id
        @transaction.delete OBJECT_AND_TAG, object_id + tag_id
        new_tag_count = number_from_bytes(@transaction.get(TAG_TO_OBJECTS_COUNT, tag_id).not_nil!) - 1
        if new_tag_count > 0
          @transaction.set TAG_TO_OBJECTS_COUNT, tag_id, number_to_bytes new_tag_count
        else
          @transaction.delete TAG_TO_OBJECTS_COUNT, tag_id
          @transaction.delete IDS_TO_SOURCES, tag_id if tag.is_a? Bytes
        end
        tags_removed_from_object += 1
      end
      if tags_removed_from_object == number_from_bytes @transaction.get(OBJECT_TO_TAGS_COUNT, object_id).not_nil!
        @transaction.delete OBJECT_TO_TAGS_COUNT, object_id
        @transaction.delete IDS_TO_SOURCES, object_id if object.is_a? Bytes
      end
      self
    end

    def []?(id : Id)
      Log.debug { "#{self.class}[#{id.value.hexstring}]?" }
      @transaction.get IDS_TO_SOURCES, id.value
    end

    def has_tag?(object : Bytes | Id, tag : Bytes | Id)
      Log.debug { "#{self.class}.has_tag? object: #{object.is_a?(Bytes) ? object.hexstring : object.value.hexstring}, tag: #{tag.is_a?(Bytes) ? tag.hexstring : tag.value.hexstring}" }
      object_id = object.is_a?(Bytes) ? Dream.digest(object) : object.value
      tag_id = tag.is_a?(Bytes) ? Dream.digest(tag) : tag.value
      @transaction.get(OBJECT_AND_TAG, object_id + tag_id) != nil
    end

    def get(object : Bytes | Id, &)
      Log.debug { "#{self.class}.get #{object.is_a?(Bytes) ? object.hexstring : object.value.hexstring}" }
      object_id = object.is_a?(Bytes) ? Dream.digest(object) : object.value
      @transaction.cursor(OBJECT_AND_TAG, from: object_id).each_next do |current_object_to_tag, _|
        current_object_id = current_object_to_tag[..15]
        break unless current_object_id == object_id
        current_tag_id = current_object_to_tag[16..]
        yield Id.new current_tag_id
      end
    end

    def get(object : Bytes | Id) : Array(Id)
      result = [] of Id
      get(object) { |tag_id| result << tag_id }
      result
    end

    def find(present_tags : Array(Bytes | Id), absent_tags : Array(Bytes | Id) = [] of Bytes | Id, start_after_object : Id? = nil, &)
      Log.debug { "#{self.class}.find present_tags: #{present_tags.map { |tag| tag.is_a?(Bytes) ? tag.hexstring : tag.value.hexstring }}, absent_tags: #{absent_tags.map { |tag| tag.is_a?(Bytes) ? tag.hexstring : tag.value.hexstring }}, start_after_object: #{start_after_object ? start_after_object.value.hexstring : nil}" }

      absent_tags_ids = absent_tags.compact_map { |tag| tag.is_a?(Bytes) ? Dream.digest(tag) : tag.value }
      absent_tags_ids.sort_by! { |tag_id| number_from_bytes @transaction.get(TAG_TO_OBJECTS_COUNT, tag_id).not_nil! rescue UInt32::MAX }
      absent_tags_ids.reverse!

      present_tags_ids = present_tags.map { |tag| tag.is_a?(Bytes) ? Dream.digest(tag) : tag.value }
      present_tags_ids.sort_by! { |tag_id| number_from_bytes @transaction.get(TAG_TO_OBJECTS_COUNT, tag_id).not_nil! rescue return }

      if present_tags_ids.size == 1
        tag_id = present_tags_ids.first
        @transaction.cursor(TAG_AND_OBJECT, from: start_after_object ? tag_id + start_after_object.value : tag_id, including_from: start_after_object.nil?).each_next do |current_tag_to_object, _|
          current_tag_id = current_tag_to_object[..15]
          break unless current_tag_id == tag_id
          current_object_id = current_tag_to_object[16..]
          yield Id.new(current_object_id) if absent_tags_ids.all? { |tag_id| @transaction.get(TAG_AND_OBJECT, tag_id + current_object_id) == nil }
        end
        return
      end

      cursors = [] of Lawn::Transaction::Cursor(Int64) # TAG_AND_OBJECT

      index_1 = 0
      index_2 = 1
      loop do
        if cursors.size == present_tags_ids.size && cursors.all? { |cursor| cursor.keyvalue.not_nil![0][16..] == cursors.first.keyvalue.not_nil![0][16..] }
          yield Id.new(cursors.first.keyvalue.not_nil![0][16..]) if absent_tags_ids.all? { |tag_id| @transaction.get(TAG_AND_OBJECT, tag_id + cursors.first.keyvalue.not_nil![0][16..]) == nil }
          return unless cursors.first.next && (cursors.first.keyvalue.not_nil![0][..15] == present_tags_ids.first)
          index_1 = 0
          index_2 = 1
        end

        if (cursors.size < present_tags_ids.size) && (cursors.size <= index_1)
          if index_1 == 0
            cursor = @transaction.cursor TAG_AND_OBJECT, from: start_after_object ? present_tags_ids[index_1] + start_after_object.value : present_tags_ids[index_1], including_from: start_after_object.nil?
          else
            cursor = @transaction.cursor TAG_AND_OBJECT, from: cursors.last.keyvalue.not_nil![0][16..]
          end
          cursor.next
          return unless cursor.keyvalue && (cursor.keyvalue.not_nil![0][..15] == present_tags_ids[index_1])
          cursors << cursor.as Lawn::Transaction::Cursor(Int64)
        end
        cursor_1 = cursors[index_1]

        if (cursors.size < present_tags_ids.size) && (cursors.size <= index_2)
          cursor = @transaction.cursor TAG_AND_OBJECT, from: present_tags_ids[index_2] + cursors.last.keyvalue.not_nil![0][16..]
          return unless cursor.next && (cursor.keyvalue.not_nil![0][..15] == present_tags_ids[index_2])
          cursors << cursor.as Lawn::Transaction::Cursor(Int64)
        end
        cursor_2 = cursors[index_2]

        until cursor_2.keyvalue.not_nil![0][16..] >= cursor_1.keyvalue.not_nil![0][16..]
          return unless cursor_2.next && (cursor_2.keyvalue.not_nil![0][..15] == present_tags_ids[index_2])
        end
        if cursor_2.keyvalue.not_nil![0][16..] >= cursor_1.keyvalue.not_nil![0][16..]
          index_1 = (index_1 + 1) % present_tags_ids.size
          index_2 = (index_2 + 1) % present_tags_ids.size
        else
          until cursors.first.keyvalue.not_nil![0][16..] >= cursors[index_2].keyvalue.not_nil![0][16..]
            return unless cursors.first.next && (cursors.first.keyvalue.not_nil![0][..15] == present_tags_ids.first)
          end
          index_1 = 0
          index_2 = 1
        end
      end
    end

    def find(present_tags : Array(Bytes | Id), absent_tags : Array(Bytes | Id) = [] of Bytes, limit : Int32 = Int32::MAX, start_after_object : Id? = nil) : Array(Id)
      result = [] of Id
      find(present_tags, absent_tags, start_after_object) do |object_id|
        break if result.size == limit
        result << object_id
      end
      result
    end

    def commit
      @transaction.commit
    end
  end

  class Index
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter database : Lawn::Database

    def initialize(@database)
    end

    def clear
      @database.clear
    end

    def transaction
      Transaction.new @database.transaction
    end

    def transaction(&)
      result = transaction
      yield result
      result.commit
    end
  end
end
