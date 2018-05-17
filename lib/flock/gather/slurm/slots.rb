require 'colorize'
require 'yaml'
require 'flock/gather/comms'
require 'sparkr'


module Flock
  module Gather
    module Slurm
      module Slots
        class << self
          EX_LINE = <<TEXT
NodeName=%PREFIX%%INDEX% Arch=x86_64 CoresPerSocket=8 CPUAlloc=%ALLOC% CPUErr=0 CPUTot=%TOTAL% CPULoad=0.02 AvailableFeatures=(null) ActiveFeatures=(null) Gres=gpu:4 NodeAddr=%PREFIX%%INDEX% NodeHostName=%PREFIX%%INDEX% Version=17.02 OS=Linux RealMemory=512000 AllocMem=28500 FreeMem=510505 Sockets=2 Boards=1 State=MIXED ThreadsPerCore=1 TmpDisk=0 Weight=1 Owner=N/A MCS_label=N/A Partitions=gpu  BootTime=2018-04-24T17:18:32 SlurmdStartTime=2018-04-25T08:43:36 CfgTRES=cpu=16,mem=500G AllocTRES=cpu=3,mem=28500M CapWatts=n/a CurrentWatts=0 LowestJoules=0 ConsumedJoules=0 ExtSensorsJoules=n/s ExtSensorsWatts=0 ExtSensorsTemp=n/s
TEXT
          EX_MATCH = /\%[^\%]+\%/

          MATCHERS = [
            /^phi/,
            /^login/,
            /^viz/,
          ]

          def skip?(name)
            MATCHERS.any? do |m|
              m.match(name)
            end
          end

          def each_line(&block)
            case ENV['FLOCK_GATHER_MODE']
            when nil
              IO.popen(['scontrol','-o','show','node']) do |io|
                while !io.eof? do
                  block.call(io.readline)
                end
              end
            when 'fixture'
              IO.popen(['cat','/tmp/sacct-data/show-node.txt']) do |io|
                while !io.eof? do
                  block.call(io.readline)
                end
              end
            when 'generated'
              base_busyness = (rand * 5).to_i * 3
              (1..10).each do |i|
                busyness = base_busyness + (rand * 8).to_i
                replacements = {
                  '%PREFIX%' => 'node',
                  '%INDEX%' => sprintf('%02d',i),
                  '%ALLOC%' => busyness > 16 ? 16 : busyness,
                  '%TOTAL%' => 16,
                }
                puts replacements['%ALLOC%']
                block.call(EX_LINE.gsub(EX_MATCH, replacements))
              end
            end
          end

          def spark_cell(line1, line2, data, key)
            vals = []
            data.each do |a|
              vals << (a[key] > 0 ? a[key] : 0)
            end
            puts vals.inspect
            avg = (vals.reduce(:+) / vals.length)
            max = vals.max
            vals << 0
            vals << data.first[:total]
            sparkline = Sparkr::Sparkline.new(vals)
            sparkline.format do |tick, count, index|
              if index >= vals.length - 2
                nil
              elsif count == max
                tick.red
              elsif count > avg
                tick.yellow
              else
                tick.green
              end
            end

            line1.to_s.cyan << "\n" <<
              line2.to_s.cyan << ' ' <<
              sparkline.to_s
          end

          def run
            total = 0
            alloc = 0
            load = 0.0

            each_line do |line|
              break if line == 'No nodes in the system'
              vals = line.split(' ')
              vals.each do |v|
                key, val = v.split('=')
                break if key == 'NodeName' && skip?(val)
                if key == 'CPUTot'
                  total += val.to_i
                elsif key == 'CPUAlloc'
                  alloc += val.to_i
                elsif key == 'CPULoad'
                  load += val.to_f
                end
              end
            end

            if total > 0
              load_avg = load / total
              puts "#{alloc}/#{total} (#{sprintf('%.2f%', (alloc * 100.0) / total)}) [#{sprintf('%.2f',load_avg)}]"
            end
              
            # read last value, increment sparkline
            hist = YAML.load_file('slot-usage-history.yml') rescue []
            hist.pop
            hist.unshift({
                           alloc: alloc,
                           total: total
                         })
            while hist.length < 12
              hist.push({
                          alloc: 0,
                          total: total
                        })
            end
            File.write('slot-usage-history.yml',hist.to_yaml)
            slot_utilization = "#{alloc}/#{total} [#{Time.now.strftime("%H:%M")}]"
            if total > 0
              pct_utilization = "#{sprintf('%.1f%', (alloc * 100.0) / total)} utilized"
            else
              pct_utilization = "No slots available"
            end
            if slot_utilization.length > pct_utilization.length
              slot_padding = ' ' * (hist.length + 1)
              pct_padding = ' ' * (slot_utilization.length - pct_utilization.length)
            else
              slot_padding = ' ' * (hist.length + 1)
              pct_padding = ' '
            end
            Flock::Gather::Comms.
              new(Gather.endpoint).
              set('usage.capacity',
                   spark_cell(
                     slot_utilization + slot_padding,
                     pct_utilization + pct_padding,
                     hist,
                     :alloc
                   ),
                   Gather.endpoint_auth)
          end
        end
      end
    end
  end
end

Flock::Gather.register('slurm/slots', Flock::Gather::Slurm::Slots)
