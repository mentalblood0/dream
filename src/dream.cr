require "log"
require "yaml"

require "lawn/Database"
require "xxhash128"

module Dream
  def self.digest(source : Bytes)
    digest = LibXxhash.xxhash128 source, source.size, 0
    result = Bytes.new 16
    IO::ByteFormat::BigEndian.encode digest.high64, result[0..7]
    IO::ByteFormat::BigEndian.encode digest.low64, result[8..15]
    result
  end

  class Index
    include YAML::Serializable
    include YAML::Serializable::Strict

    record Id, value : Bytes

    TAGS_TO_OBJECTS = 0_u8
    OBJECTS_TO_TAGS = 1_u8
    IDS_TO_SOURCES  = 2_u8

    class Transaction
      getter transaction : Lawn::Transaction

      def initialize(@transaction)
      end

      def add(object : Bytes | Id, tags : Array(Bytes | Id))
        Log.debug { "#{self.class}.add object: #{object.is_a?(Bytes) ? object.hexstring : object.value.hexstring}, tags: #{tags.map { |tag| tag.is_a?(Bytes) ? tag.hexstring : tag.value.hexstring }}" }
        object_id = object.is_a?(Bytes) ? Dream.digest(object) : object.value
        @transaction.set IDS_TO_SOURCES, object_id, object if object.is_a? Bytes
        tags.each do |tag|
          tag_id = tag.is_a?(Bytes) ? Dream.digest(tag) : tag.value
          @transaction.set TAGS_TO_OBJECTS, tag_id + object_id
          @transaction.set OBJECTS_TO_TAGS, object_id + tag_id
          @transaction.set IDS_TO_SOURCES, tag_id, tag if tag.is_a? Bytes
        end
        self
      end

      def delete(object : Bytes | Id)
        Log.debug { "#{self.class}.delete #{object.is_a?(Bytes) ? object.hexstring : object.value.hexstring}" }
        object_id = object.is_a?(Bytes) ? Dream.digest(object) : object.value
        @transaction.delete IDS_TO_SOURCES, object_id if object.is_a? Bytes
        @transaction.database.tables[OBJECTS_TO_TAGS].each(from: object_id) do |current_object_to_tag, _|
          current_object_id = current_object_to_tag[..15]
          break unless current_object_id == object_id
          current_tag_id = current_object_to_tag[16..]
          @transaction.delete TAGS_TO_OBJECTS, current_tag_id + current_object_id
          @transaction.delete OBJECTS_TO_TAGS, current_object_to_tag
        end
        self
      end

      def delete(object : Bytes | Id, tags : Array(Bytes | Id))
        Log.debug { "#{self.class}.delete object: #{object.is_a?(Bytes) ? object.hexstring : object.value.hexstring}, tags: #{tags.map { |tag| tag.is_a?(Bytes) ? tag.hexstring : tag.value.hexstring }}" }
        object_id = object.is_a?(Bytes) ? Dream.digest(object) : object.value
        tags.each do |tag|
          tag_id = tag.is_a?(Bytes) ? Dream.digest(tag) : tag.value
          @transaction.delete TAGS_TO_OBJECTS, tag_id + object_id
          @transaction.delete OBJECTS_TO_TAGS, object_id + tag_id
        end
        @transaction.database.tables[OBJECTS_TO_TAGS].each(from: object_id) do |current_object_to_tag, _|
          current_object_id = current_object_to_tag[..15]
          if current_object_id == object_id
            return self
          else
            break
          end
        end
        @transaction.delete IDS_TO_SOURCES, object_id
        self
      end

      def commit
        @transaction.commit
      end
    end

    getter database : Lawn::Database

    def initialize(@database)
    end

    def transaction
      Transaction.new @database.transaction
    end

    def []?(id : Id)
      Log.debug { "#{self.class}[#{id.value.hexstring}]?" }
      @database.tables[IDS_TO_SOURCES].get id.value
    end

    def has_tag?(object : Bytes | Id, tag : Bytes | Id)
      Log.debug { "#{self.class}.has_tag? object: #{object.is_a?(Bytes) ? object.hexstring : object.value.hexstring}, tag: #{tag.is_a?(Bytes) ? tag.hexstring : tag.value.hexstring}" }
      object_id = object.is_a?(Bytes) ? Dream.digest(object) : object
      tag_id = tag.is_a?(Bytes) ? Dream.digest(tag) : tag
      @database.tables[OBJECTS_TO_TAGS].get(object_id + tag_id) != nil
    end

    def get(object : Bytes | Id, &)
      Log.debug { "#{self.class}.get #{object.is_a?(Bytes) ? object.hexstring : object.value.hexstring}" }
      object_id = object.is_a?(Bytes) ? Dream.digest(object) : object
      @database.tables[OBJECTS_TO_TAGS].each(from: object_id) do |current_object_to_tag, _|
        current_object_id = current_object_to_tag[..15]
        break unless current_object_id == object_id
        current_tag_id = current_object_to_tag[16..]
        yield current_tag_id
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
      present_tags_ids = present_tags.map { |tag| tag.is_a?(Bytes) ? Dream.digest(tag) : tag.value }

      if present_tags_ids.size == 1
        tag_id = present_tags_ids.first
        @database.tables[TAGS_TO_OBJECTS].each(from: start_after_object ? tag_id + start_after_object : tag_id) do |current_tag_to_object, _|
          current_tag_id = current_tag_to_object[..15]
          break unless current_tag_id == tag_id
          current_object_id = current_tag_to_object[16..]
          yield Id.new(current_object_id) if absent_tags_ids.all? { |tag_id| @database.tables[TAGS_TO_OBJECTS].get(tag_id + current_object_id) == nil }
        end
        return
      end

      cursors = [] of Lawn::Table::Cursor(Int64) # TAGS_TO_OBJECTS

      index_1 = 0
      index_2 = 1
      loop do
        if cursors.size == present_tags_ids.size && cursors.all? { |cursor| cursor.keyvalue.not_nil![0][16..] == cursors.first.keyvalue.not_nil![0][16..] }
          yield Id.new(cursors.first.keyvalue.not_nil![0][16..]) if absent_tags_ids.all? { |tag_id| @database.tables[TAGS_TO_OBJECTS].get(tag_id + cursors.first.keyvalue.not_nil![0][16..]) == nil }
          return unless cursors.first.next && (cursors.first.keyvalue.not_nil![0][..15] == present_tags_ids.first)
          index_1 = 0
          index_2 = 1
        end

        if (cursors.size < present_tags_ids.size) && (cursors.size <= index_1)
          if index_1 == 0
            cursor = @database.tables[TAGS_TO_OBJECTS].cursor start_after_object ? present_tags_ids[index_1] + start_after_object : present_tags_ids[index_1]
          else
            cursor = @database.tables[TAGS_TO_OBJECTS].cursor cursors.last.keyvalue.not_nil![0][16..]
          end
          cursor.next
          return unless cursor.keyvalue && (cursor.keyvalue.not_nil![0][..15] == present_tags_ids[index_1])
          cursors << cursor.as Lawn::Table::Cursor(Int64)
        end
        cursor_1 = cursors[index_1]

        if (cursors.size < present_tags_ids.size) && (cursors.size <= index_2)
          cursor = @database.tables[TAGS_TO_OBJECTS].cursor present_tags_ids[index_2] + cursors.last.keyvalue.not_nil![0][16..]
          return unless cursor.next && (cursor.keyvalue.not_nil![0][..15] == present_tags_ids[index_2])
          cursors << cursor.as Lawn::Table::Cursor(Int64)
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

    def find(present_tags : Array(Bytes | Id), absent_tags : Array(Bytes | Id) = [] of Bytes, limit : UInt64 = UInt64::MAX, start_after_object : Id? = nil) : Array(Id)
      result = [] of Id
      find(present_tags, absent_tags, start_after_object) do |object_id|
        break if result.size == limit
        result << object_id
      end
      result
    end
  end
end
