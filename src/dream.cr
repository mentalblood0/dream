require "sophia"

module Dream
  Sophia.define_env Env, {t2o: {key: {t2ot: UInt32,
                                      t2oo: UInt32}},
                          o2t: {key: {o2to: UInt32,
                                      o2tt: UInt32}},
                          i2t: {key: {i2ti: UInt32},
                                value: {i2tt: String}},
                          t2i: {key: {t2it: String},
                                value: {t2ii: UInt32}},
                          i2o: {key: {i2oi: UInt32},
                                value: {i2oo: String}},
                          o2i: {key: {o2io: String},
                                value: {o2ii: UInt32}},
                          c: {key: {ti: UInt32},
                              value: {c: UInt32}}}

  class Index
    @tc : UInt32
    @oc : UInt32

    def initialize(@sophia : Env)
      @tc = (@sophia.cursor({i2ti: UInt32::MAX}, "<=").next.not_nil![:i2ti] rescue 0_u32) + 1
      @oc = (@sophia.cursor({i2oi: UInt32::MAX}, "<=").next.not_nil![:i2oi] rescue 0_u32) + 1
    end

    def add(object : String, tags : Array(String))
      @sophia.transaction do |tx|
        oi = (@sophia[{o2io: object}]?.not_nil![:o2ii] rescue begin
          tx << {o2io: object, o2ii: @oc}
          tx << {i2oi: @oc, i2oo: object}
          @oc += 1
          @oc - 1
        end)
        tags.each do |tag|
          ti = (@sophia[{t2it: tag}]?.not_nil![:t2ii] rescue begin
            tx << {t2it: tag, t2ii: @tc}
            tx << {i2ti: @tc, i2tt: tag}
            @tc += 1
            @tc - 1
          end)
          tx << {t2ot: ti, t2oo: oi}
          tx << {o2to: oi, o2tt: ti}
          tx << {ti: ti, c: (@sophia[{ti: ti}]?.not_nil![:c] rescue 0_u32) + 1}
        end
      end
    end

    def delete(object : String)
      oi = @sophia[{o2io: object}]?.not_nil![:o2ii] rescue return
      @sophia.transaction do |tx|
        @sophia.from({o2to: oi, o2tt: 0_u32}) do |o2t|
          tx.delete({t2ot: o2t[:o2tt], t2oo: oi})
          tx.delete({o2to: oi, o2tt: o2t[:o2tt]})
        end
        tx.delete({o2io: object})
        tx.delete({i2oi: oi})
      end
    end

    def delete(object : String, tags : Array(String))
      oi = @sophia[{o2io: object}]?.not_nil![:o2ii] rescue return
      @sophia.transaction do |tx|
        tags.each do |t|
          ti = @sophia[{t2it: t}]?.not_nil![:t2ii] rescue next
          tx.delete({t2ot: ti, t2oo: oi})
          tx.delete({o2to: oi, o2tt: ti})
        end
      end
    end

    def find(present : Array(String), absent : Array(String) = [] of String, limit : UInt32 = UInt32::MAX, from : String? = nil)
      fromi = if from
                @sophia[{o2io: from}]?.not_nil![:o2ii]
              else
                nil
              end

      ais = absent.compact_map { |t| @sophia[{t2it: t}]?.not_nil![:t2ii] rescue nil }
      ais.sort_by! { |ti| @sophia[{ti: ti}]?.not_nil![:c] }
      ais.reverse!

      r = [] of String
      if present.size == 1
        ti = @sophia[{t2it: present.first}]?.not_nil![:t2ii] rescue return r
        @sophia.from({t2ot: ti, t2oo: (fromi.not_nil! rescue 0_u32)}, ">") do |t2o|
          break if r.size == limit || t2o[:t2ot] != ti
          r << @sophia[{i2oi: t2o[:t2oo]}]?.not_nil![:i2oo] if ais.all? { |ai| !@sophia.has_key?({t2ot: ai, t2oo: t2o[:t2oo]}) }
        end
        return r
      end

      pis = present.map { |t| @sophia[{t2it: t}]?.not_nil![:t2ii] rescue return r }
      pis.sort_by! { |ti| @sophia[{ti: ti}]?.not_nil![:c] }

      cs = [] of Dream::Env::T2oCursor

      i1 = 0
      i2 = 1
      loop do
        if cs.size == present.size && cs.all? { |c| c.data.not_nil![:t2oo] == cs.first.data.not_nil![:t2oo] }
          if ais.all? { |ai| !@sophia.has_key?({t2ot: ai, t2oo: cs.first.data.not_nil![:t2oo]}) }
            r << @sophia[{i2oi: cs.first.data.not_nil![:t2oo]}]?.not_nil![:i2oo]
            return r if r.size == limit
          end
          return r unless cs.first.next && cs.first.data.not_nil![:t2ot] == pis.first
          i1 = 0
          i2 = 1
        end

        if cs.size < present.size && cs.size <= i1
          if i1 == 0
            c = @sophia.cursor({t2ot: pis[i1], t2oo: (fromi.not_nil! rescue 0_u32)}, ">")
          else
            c = @sophia.cursor({t2ot: pis[i1], t2oo: cs.last.data.not_nil![:t2oo]})
          end
          return r unless c.next && c.data.not_nil![:t2ot] == pis[i1]
          cs << c
        end
        c1 = cs[i1]

        if cs.size < present.size && cs.size <= i2
          c = @sophia.cursor({t2ot: pis[i2], t2oo: cs.last.data.not_nil![:t2oo]})
          return r unless c.next && c.data.not_nil![:t2ot] == pis[i2]
          cs << c
        end
        c2 = cs[i2]

        until c2.data.not_nil![:t2oo] >= c1.data.not_nil![:t2oo]
          return r unless c2.next && c2.data.not_nil![:t2ot] == pis[i2]
        end
        if c2.data.not_nil![:t2oo] == c1.data.not_nil![:t2oo]
          i1 = (i1 + 1) % present.size
          i2 = (i2 + 1) % present.size
        else
          until cs.first.data.not_nil![:t2oo] >= cs[i2].data.not_nil![:t2oo]
            return r unless cs.first.next && cs.first.data.not_nil![:t2ot] == pis.first
          end
          i1 = 0
          i2 = 1
        end
      end
    end
  end
end
