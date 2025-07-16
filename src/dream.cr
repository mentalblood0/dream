require "sophia"

module Dream
  alias Oid = Tuple(UInt64, UInt64)

  Sophia.define_env DreamEnv, {tags: {key: {tag: String,
                                            oid0: UInt64,
                                            oid1: UInt64}}}

  class Index
    def initialize(path : String, opts : Sophia::H = Sophia::H{"compression"      => "zstd",
                                                               "compaction.cache" => 2_i64 * 1024 * 1024 * 1024})
      @sophia = DreamEnv.new Sophia::H{"sophia.path" => path}, {tags: opts}
    end

    def add(oid : Oid, tags : Array(String))
      @sophia << tags.map { |tag| {tag: tag, oid0: oid[0], oid1: oid[1]} } unless @sophia.has_key?({tag: tags.first, oid0: oid[0], oid1: oid[1]})
    end

    protected def moe(a : DreamEnv::TagsCursor, b : DreamEnv::TagsCursor)
      (a.data.not_nil![:oid0] == b.data.not_nil![:oid0] && a.data.not_nil![:oid1] >= b.data.not_nil![:oid1]) ||
        (a.data.not_nil![:oid0] > b.data.not_nil![:oid0])
    end

    def find(tags : Array(String), limit : UInt64 = UInt64::MAX)
      r = [] of Oid

      cs = [] of DreamEnv::TagsCursor
      tags.each do |tag|
        cs << @sophia.cursor({tag: tag, oid0: (cs.last.data.not_nil![:oid0] rescue 0_u64), oid1: (cs.last.data.not_nil![:oid1] rescue 0_u64)})
        return r unless cs.last.next && cs.last.data.not_nil![:tag] == tag
      end

      loop do
        r << {cs.first.data.not_nil![:oid0], cs.first.data.not_nil![:oid1]} if cs.all? { |c| c.data.not_nil![:oid0] == cs.first.data.not_nil![:oid0] && c.data.not_nil![:oid1] == cs.first.data.not_nil![:oid1] }
        return r if r.size == limit
        loop do
          return r unless cs.first.next && cs.first.data.not_nil![:tag] == tags.first
          break if moe cs.first, cs.last
        end
        i = 1
        cs.each_cons_pair do |c1, c2|
          until moe c2, c1
            return r unless c2.next && c2.data.not_nil![:tag] == tags[i]
          end
          i += 1
        end
      end
      r
    end

    def clear
      @sophia.from({tag: "", oid0: 0_u64, oid1: 0_u64}) { |rec| @sophia.delete rec }
    end
  end
end
