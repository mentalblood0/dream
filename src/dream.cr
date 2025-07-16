require "sophia"

module Dream
  Sophia.define_env DreamEnv, {ii: {key: {ti: UInt32,
                                          oi: UInt32}},
                               i2t: {key: {i2ti: UInt32},
                                     value: {i2tt: String}},
                               t2i: {key: {t2it: String},
                                     value: {t2ii: UInt32}},
                               i2o: {key: {i2oi: UInt32},
                                     value: {i2oo: String}},
                               o2i: {key: {o2io: String},
                                     value: {o2ii: UInt32}}}

  class Index
    @tc : UInt32
    @oc : UInt32

    def initialize(path : String, opts : Sophia::H = Sophia::H{"compression"      => "zstd",
                                                               "compaction.cache" => 2_i64 * 1024 * 1024 * 1024})
      @sophia = DreamEnv.new Sophia::H{"sophia.path" => path}, {ii: opts, i2t: opts, t2i: opts, i2o: opts, o2i: opts}
      @tc = (@sophia.cursor({i2ti: UInt32::MAX}, "<=").next.not_nil![:i2ti] + 1 rescue 0_u32)
      @oc = (@sophia.cursor({i2oi: UInt32::MAX}, "<=").next.not_nil![:i2oi] + 1 rescue 0_u32)
    end

    def add(object : String, tags : Array(String))
      return if @sophia.has_key?({o2io: object})
      ltc = @tc
      loc = @oc
      @sophia.transaction do |tx|
        tx << {o2io: object, o2ii: loc}
        tx << {i2oi: loc, i2oo: object}
        oi = loc
        loc += 1
        tags.each do |tag|
          ti = (tx[{t2it: tag}]?.not_nil![:t2ii] rescue begin
            tx << {t2it: tag, t2ii: ltc}
            tx << {i2ti: ltc, i2tt: tag}
            ltc += 1
            ltc - 1
          end)
          tx << {ti: ti, oi: oi}
        end
      end
      @tc = ltc
      @oc = loc
    end

    def find(tags : Array(String), limit : UInt64 = UInt64::MAX)
      r = [] of String

      tis = tags.map { |t| @sophia[{t2it: t}]?.not_nil![:t2ii] rescue return r }

      cs = [] of DreamEnv::IiCursor
      tis.each do |ti|
        c = @sophia.cursor({ti: ti, oi: (cs.last.data.not_nil![:oi] rescue 0_u32)})
        return r unless c.next && c.data.not_nil![:ti] == ti
        cs << c
      end

      loop do
        r << @sophia[{i2oi: cs.first.data.not_nil![:oi]}]?.not_nil![:i2oo] if cs.all? { |c| c.data.not_nil![:oi] == cs.first.data.not_nil![:oi] }
        return r if r.size == limit
        loop do
          return r unless cs.first.next && cs.first.data.not_nil![:ti] == tis.first
          break if cs.first.data.not_nil![:oi] >= cs.last.data.not_nil![:oi]
        end
        i = 1
        cs.each_cons_pair do |c1, c2|
          until c2.data.not_nil![:oi] >= c1.data.not_nil![:oi]
            return r unless c2.next && c2.data.not_nil![:ti] == tis[i]
          end
          i += 1
        end
      end
      r
    end
  end
end
