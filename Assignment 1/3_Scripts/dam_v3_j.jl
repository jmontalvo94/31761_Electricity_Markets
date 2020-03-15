## Import packages

using DataFrames
using DataFramesMeta
using Plots
using JuMP
using Gurobi
using MathOptFormat
using LinearAlgebra
using Printf
using CSV
using Dates
gr()

## Define functions

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

function check_status!(G_1, G_2, D_1, D_2)
	if (G_1 >= D_1 && G_2 >= D_2)
		status = "Stable"
	elseif (G_1 >= D_1 && G_2 < D_2)
		status = "Shortage in DK2"
	elseif (G_1 < D_1 && G_2 >= D_2)
		status = "Shortage in DK1"
	elseif (G_1 < D_1 && G_2 < D_2)
		status = "Shortage"
	end
	return status
end

function initialize_data!(t, df, P_G_DK1, P_G_DK2)
	# Initialize wind and nuclear production (between 5am and 10pm only)
	if (df.Hour[t] > 5 && df.Hour[t] < 23)
		P_G_DK1[6] = 900.0
		P_G_DK2[1] = 1100.0
	else
		P_G_DK1[6] = 0.0
		P_G_DK2[1] = 0.0
	end

	# Set wind power bid at t
	P_G_DK1[8] = df.WestWind₁[t]
	P_G_DK1[9] = df.WestWind₂[t]
	P_G_DK2[9] = df.EastWind₂[t]

	# Set total bids
	b_DK1 = [P_G_DK1; df.DK1[t] + df.DK1_Import[t] + df.DK1_Export[t]]
	b_DK2 = [P_G_DK2; df.DK2[t] + df.DK2_Import[t] - df.EastWind₁[t]]
	return b_DK1, b_DK2
end

function print_results(model_fbdam)
	if termination_status(model_fbdam) == MOI.OPTIMAL
	    println("Optimal solution found!\n")
	    println("Generation and Demand:\n")
	    for j in n_DK1
	        println("$(id_DK1[j]): ", value.(y_DK1[j]), " MWh")
	    end
	    for i in n_DK2
	        println("$(id_DK2[i]): ", value.(y_DK2[i]), " MWh")
	    end
	    println("\nObjective value: ", objective_value(model_fbdam), " €")
	    println("\nMarket equilibrium DK1: ", dual(balance_DK1), " €/MWh")
	    println("\nMarket equilibrium DK2: ", dual(balance_DK2), " €/MWh")
	    else
	        error("No solution.")
	end
end

function create_symbols(vector_of_strings)
	tags = Array{Symbol}(undef,length(vector_of_strings))
	for (i,s) in enumerate(vector_of_strings)
		tags[i] = Symbol(s)
	end
	return tags
end

## Data cleaning

# Set file names (CSV files were used insted of xls)
files = ["consumption-prognosis_2019_hourly.csv","consumption-prognosis_2020_hourly.csv",
		"wind-power-dk-prognosis_2019_hourly.csv","wind-power-dk-prognosis_2020_hourly.csv"]

# Set imports and Exports
imp_DK1_NO = -100 # [MW]
exp_DK1_DE = 120 # [MW] from 8am to 3pm only
imp_DK2_SE = -80 # [MW] from 11am to 5pm only
impexp = [imp_DK1_NO, exp_DK1_DE, imp_DK2_SE]

# Read files into separate DataFrames
dfs_consumption = [read_file(files[1]),read_file(files[2])]
dfs_wind = [read_file(files[3]),read_file(files[4])]

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

## Initialize input vectors

# Suppliers per BZ
supplier_DK1 = 	["FlexiGas","FlexiGas","FlexiGas","Peako","Peako","Nuke22","CoalAtLast",
				"WestWind₁", "WestWind₂"]
supplier_DK2 = 	["Nuke22","RoskildeCHP","RoskildeCHP","Avedøvre","Avedøvre","BlueWater",
				"BlueWater","CoalAtLast", "EastWind₂"]
id_DK1 = ["G₁","G₂","G₃","G₄","G₅","G₆","G₇", "WW₁", "WW₂", "DK1"]
id_DK2 = ["G₈","G₉","G₁₀","G₁₁","G₁₂","G₁₃","G₁₄","G₁₅", "EW₂", "DK2"]

# Transmission capacity between DK1 and DK2
t_capacity = 600 # [MW]

# Time periods
T = collect(1:size(df,1))

# Supplier vector size
N_G_DK1 = size(supplier_DK1,1)
N_G_DK2 = size(supplier_DK2,1)

# Supplier vector counter
n_G_DK1 = collect(1:N_G_DK1)
n_G_DK2 = collect(1:N_G_DK2)

# Total vector size
N_DK1 = N_G_DK1+1
N_DK2 = N_G_DK2+1

# Total vector counter
n_DK1 = collect(1:N_DK1)
n_DK2 = collect(1:N_DK2)

# Maximum capacities
P_G_DK1  = [380.0,350.0,320.0,370.0,480.0,900.0,1200.0, 0.0, 0.0]
P_G_DK2  = [1100.0,300.0,380.0,360.0,320.0,750.0,600.0,860.0, 0.0]

# Supplier bid price
λ_G_DK1 = [72,62,150,80,87,24,260,0,-17]
λ_G_DK2 = [17,44,40,37,32,5,12,235,-12]

# Total bids
c_DK1 = [λ_G_DK1; 0] # Generators plus demand at zero price
c_DK2 = [λ_G_DK2; 0] # Generators plus demand at zero price

# Helping matrices
A_eq_DK1 = transpose([ones(N_G_DK1);-1])
A_eq_DK2 = transpose([ones(N_G_DK2);-1])
A_DK1 = Array(Diagonal(ones(N_DK1)))
A_DK2 = Array(Diagonal(ones(N_DK2)))

## Model

# Initialize output DataFrame
tags_results = create_symbols(["SystemCost"; "λₛ_DK1"; "λₛ_DK2"; "DK1"; "HVDC"; "DK2";
 								"G_DK1"; "W_DK1"; "G_DK2"; "W_DK2"])
tags_results_DK1 = create_symbols(["SystemCost"; "λₛ_DK1"; "HVDC"; id_DK1[end]; id_DK1[1:end-1]])
tags_results_DK2 = create_symbols(["SystemCost"; "λₛ_DK2"; "HVDC"; id_DK2[end]; id_DK2[1:end-1]])
df_results = DataFrame(fill(Float64,length(tags_results)), tags_results, 0)
df_results_DK1 = DataFrame(fill(Float64,length(tags_results_DK1)), tags_results_DK1, 0)
df_results_DK2 = DataFrame(fill(Float64,length(tags_results_DK2)), tags_results_DK2, 0)

# Run model for all T
for t in T

	# Initialize data for nuclear, wind power and set total bids vectors
	b_DK1, b_DK2 = initialize_data!(t, df, P_G_DK1, P_G_DK2)

	# Set bidding zones' status (stable, shortage in DK1, shortage in DK2 or shortage)
	status = check_status!(sum(P_G_DK1), sum(P_G_DK2), b_DK1[end], b_DK2[end])

	# Boot model
	model_fbdam = Model(with_optimizer(Gurobi.Optimizer))

	# Variables
	@variable(model_fbdam, y_DK1[j in n_DK1] >= 0)
	@variable(model_fbdam, y_DK2[j in n_DK2] >= 0)
	@variable(model_fbdam, t_capacity >= b_eq >= -t_capacity)

	# Objective function
	@objective(model_fbdam, Min, transpose(c_DK1)*y_DK1 + transpose(c_DK2)*y_DK2)

	# Maximum capacity
	@constraint(model_fbdam, maxcapacity_DK1, A_DK1*y_DK1 .<= b_DK1)
	@constraint(model_fbdam, maxcapacity_DK2, A_DK2*y_DK2 .<= b_DK2)
	# Balance equation
	@constraint(model_fbdam, balance_DK1, A_eq_DK1*y_DK1 == b_eq)
	@constraint(model_fbdam, balance_DK2, A_eq_DK2*y_DK2 == -b_eq)

	# Load shedding constraints depending on bidding zones' status
	if status == "Stable"
		@constraint(model_fbdam, stable_DK1, y_DK1[end] >= b_DK1[end])
		@constraint(model_fbdam, stable_DK2, y_DK2[end] >= b_DK2[end])
	elseif status == "Shortage in DK2"
		@constraint(model_fbdam, stable_DK1, y_DK1[end] >= b_DK1[end])
		@constraint(model_fbdam, shortage_DK2, y_DK2[end] >= sum(P_G_DK2))
	elseif status == "Shortage in DK1"
		@constraint(model_fbdam, shortage_DK1, y_DK1[end] >= sum(P_G_DK1))
		@constraint(model_fbdam, stable_DK2, y_DK2[end] >= b_DK2[end])
	elseif status == "Shortage"
		@constraint(model_fbdam, shortage_DK1, y_DK1[end] >= sum(P_G_DK1))
		@constraint(model_fbdam, shortage_DK2, y_DK2[end] >= sum(P_G_DK2))
	end

	# Solve
	optimize!(model_fbdam)

	# Add t=x results to DataFrame
	results = 	[objective_value(model_fbdam); dual(balance_DK1); dual(balance_DK2);
				value.(y_DK1)[end]; value.(b_eq); value.(y_DK2)[end];
				sum(value.(y_DK1).data[1:7]); sum(value.(y_DK1).data[8:9]);
				sum(value.(y_DK2).data[1:8]); value.(y_DK2)[9]]
	results_DK1 = 	[objective_value(model_fbdam); dual(balance_DK1); value.(b_eq);
					value.(y_DK1)[end]; value.(y_DK1).data[1:end-1]]
	results_DK2 =	[objective_value(model_fbdam); dual(balance_DK2); value.(b_eq);
					value.(y_DK2)[end]; value.(y_DK2).data[1:end-1]]
	push!(df_results, results)
	push!(df_results_DK1, results_DK1)
	push!(df_results_DK2, results_DK2)
end


# Print results on t=x for testing purposes
#print_results(model_fbdam)

# Auxiliary DataFrames to check results
df_results = hcat(select(df, Between(:Year, :Hour)), df_results)
df_results_DK1 = hcat(select(df, Between(:Year, :Hour)), df_results_DK1)
df_results_DK2 = hcat(select(df, Between(:Year, :Hour)), df_results_DK2)
df_congestion = filter(row -> row[:HVDC] == 600 || row[:HVDC] == -600, df_results)
df_revenues_DK1 = 	hcat(select(df_results_DK1, Between(:Year, :Hour)),
					select(df_results_DK1, Between(:G₁, :WW₂)).*df_results_DK1.λₛ_DK1)
df_revenues_DK2 = 	hcat(select(df_results_DK2, Between(:Year, :Hour)),
					select(df_results_DK2, Between(:G₈, :EW₂)).*df_results_DK2.λₛ_DK2)

## Export results

sheets = Dict(:df=>(collect(DataFrames.eachcol(df)), names(df)),
		:df_results=>(collect(DataFrames.eachcol(df_results)), names(df_results)),
		:df_results_DK1=>(collect(DataFrames.eachcol(df_results_DK1)), names(df_results_DK1)),
		:df_results_DK2=>(collect(DataFrames.eachcol(df_results_DK2)), names(df_results_DK2)),
		:df_congestion=>(collect(DataFrames.eachcol(df_congestion)), names(df_congestion)),
		:df_revenues_DK1=>(collect(DataFrames.eachcol(df_revenues_DK1)), names(df_revenues_DK1)),
		:df_revenues_DK2=>(collect(DataFrames.eachcol(df_revenues_DK2)), names(df_revenues_DK2)),)
XLSX.writetable("output_data.xlsx"; sheets...)
