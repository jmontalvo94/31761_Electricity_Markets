## Import packages

using CSV
using DataFrames
using Dates
using DelimitedFiles
using HTTP

## Define functions

function get_file_from(url::String, missing_str::String)
    file = HTTP.get(url)
    df = DataFrame(CSV.File(file.body; missingstring = missing_str))
    return df
end

function replace_string!(
    df::DataFrame,
    weird_str::String,
    normal_str::String,
    cols::Tuple{Integer,Integer,Integer}
)
    for col in cols
        for (i, str) in enumerate(df[!,col])
            if str == weird_str
                df[i,col] = normal_str
            end
        end
    end
    return df
end

function convert_str_to_int!(df::DataFrame, cols::Tuple{Integer,Integer,Integer})
    for col in cols
        df[!,col] = parse.(Int64, df_forecast[:,col])
    end
    return df
end

function convert_int_to_date!(df::DataFrame, cols::Tuple{Integer,Integer})
    format = DateFormat("yyyymmddHHMM")
    for col in cols
        df[!,col] = DateTime.(string.(df[!,col]), format)
    end
    return df
end

function read_file(file)
    df = CSV.read(file,header=3,datarow=4)
    df = rename!(df, :1 => "Date")
    if file[1:11] == "consumption"
        df = select!(df, Not([:NO,:SE,:FI,:EE,:LV,:LT]))
    end
    return df
end

function adjust_date!(dfs)
	for df in dfs
		newDate = fill(Date("1994-01-01"),size(df,1))
	    for (i, oldDate) in enumerate(df.Date)
			try
				newDate[i] = Date(oldDate,"dd/mm/yyyy")
			catch
				newDate[i] = Date(oldDate, "dd-mm-yyyy")
			end
		end
		select!(df, Not(:Date)) # Remove old date
		insertcols!(df,1,:Date => newDate) # Insert new date
		insertcols!(df,2,:Year => year.(newDate))
		insertcols!(df,3,:Month => month.(newDate))
		insertcols!(df,4,:Day => day.(newDate))
	end
end

function remove_months_and_merge!(dfs)
	df_merged = DataFrame()
	for df in dfs
		if df.Year[1] == 2019
			df_merged = append!(df_merged, @where(df, :Year .== 2019, :Month .== 11))
		else
			df_merged = append!(df_merged, @where(df, :Year .== 2020, :Month .== 1))
		end
	end
	dropmissing!(df_merged)
	return df_merged
end

function adjust_hours!(df)
	newHour = fill(0,size(df.Hours,1))
	for (i, oldHour) in enumerate(df.Hours)
		newHour[i] = parse(Int64,oldHour[1:2]) + 1
	end
	select!(df, Not(:Hours)) # Remove old hour
	insertcols!(df,5,:Hour => newHour) # Insert new hour
end

function adjust_consumption!(df, impexp)
	dk1_imp = impexp[1]
	dk1_exp = zeros(size(df,1))
	dk2_imp = zeros(size(df,1))
	for (i, hour) in enumerate(df.Hour)
		if (hour > 8 && hour < 16)
			dk1_exp[i] = impexp[2]
		end
		if (hour > 11 && hour < 19)
				dk2_imp[i] = impexp[3]
		end
		#df.DK1[i] = df.DK1[i] - dk1_imp + dk1_exp[i]
		#df.DK2[i] = df.DK2[i] - dk2_imp[i]
	end
	insertcols!(df,8,:DK1_Import => fill(dk1_imp,size(df.DK1,1)))
	insertcols!(df,9,:DK1_Export => dk1_exp)
	insertcols!(df,10,:DK2_Import => dk2_imp)
end

function add_wind!(df)
    insertcols!(df,8,:WestWind₁=>df.DK1.*0.8)
    insertcols!(df,9,:WestWind₂=>df.DK1.*0.2)
    insertcols!(df,10,:EastWind₁=>df.DK2.*0.1)
    insertcols!(df,11,:EastWind₂=>df.DK2.*0.9)
end

function merge_dfs!(df1, df2)
	df = copy(df1)
	for i=8:11
		insertcols!(df, i+3, names(df2)[i] => df2[!,i])
	end
	return df
end

## Load data and clean

# Fetch forecast data from URL
forecast_url = "http://pierrepinson.com/31761/Assignments/windpowerforecasts.dat"
df_forecast = get_file_from(forecast_url, "NA")

# Drop rows with missing values
dropmissing!(df_forecast)

# Replace string "1e+05" to "100000"
replace_string!(df_forecast, "1e+05", "100000", (6,16,24))

# Convert types
convert_str_to_int!(df_forecast, (6,16,24))
convert_int_to_date!(df_forecast, (1,2))

files = ["elspot-prices_2016_hourly_eur.csv","elspot-prices_2017_hourly_eur.csv",
		"regulating-prices_2016_hourly_eur.csv","regulating-prices_2017_hourly_eur.csv"]

# Read files into separate DataFrames
dfs_elspot = [read_file(files[1]),read_file(files[2])]
dfs_regulating = [read_file(files[3]),read_file(files[4])]

# Adjust date
adjust_date!(dfs_consumption)
adjust_date!(dfs_wind)

# Remove months and merge 2019 with 2020 into merged DataFrame
df_consumption = remove_months_and_merge!(dfs_consumption)
df_wind = remove_months_and_merge!(dfs_wind)

# Adjust hours
adjust_hours!(df_consumption)
adjust_hours!(df_wind)

#Adjust consumption
adjust_consumption!(df_consumption, impexp)

#Adjust wind and create offers
add_wind!(df_wind)

#Merge DataFrames into single
df = merge_dfs!(df_consumption, df_wind)
dfs_consumption = nothing
df_consumption = nothing
dfs_wind = nothing
df_wind = nothing
