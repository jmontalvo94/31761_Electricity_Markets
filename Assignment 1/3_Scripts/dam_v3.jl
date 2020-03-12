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

## Create functions

function read_file(file)
    df = CSV.read(file,header=3,datarow=4)
    df = rename!(df,:1 => "Date")
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
	return df
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

## Data cleaning

# Set file names
files=["consumption-prognosis_2019_hourly.csv","consumption-prognosis_2020_hourly.csv","wind-power-dk-prognosis_2019_hourly.csv","wind-power-dk-prognosis_2020_hourly.csv"]

# Imports and Exports
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

#=Data to arrays?
array_b = Array(df_b)
b = zeros(Int,length(M),length(F),length(T),length(S))
for i=1:size(array_b,1)
    b[array_b[i,1],array_b[i,2],array_b[i,3],array_b[i,4]]=array_b[i,5]
end
=#

## Model

# Suppliers per BZ
supplier_DK1 = ["FlexiGas","FlexiGas","FlexiGas","Peako","Peako","Nuke22","CoalAtLast"]
id_GDK1 = ["G₁","G₂","G₃","G₄","G₅","G₆","G₇"]
supplier_DK2 = ["Nuke22","RoskildeCHP","RoskildeCHP","Avedøvre","Avedøvre","BlueWater","BlueWater","CoalAtLast"]
id_GDK2 = ["G₈","G₉","G₁₀","G₁₁","G₁₂","G₁₃","G₁₄","G₁₅"]

# Transmission capacity between DK1 and DK2
t_capacity = 600 # [MW]

#creating imports and exports
en_NO = repeat([-100],24)
en_GE=zeros(24)
for i=1:24
    if (i<9 || i>15)
        en_GE[i] = 0
    else
        en_GE[i] = 120
end
end
en_SWE=zeros(24)
for i=1:24
    if (i<11 || i>17)
        en_SWE[i] = 0
    else
        en_SWE[i] = -80
end
end

# Initialize vectors
N_DK1 = size(supplier_DK1,1)
n_DK1 = collect(1:N_DK1)
N_DK2 = size(supplier_DK2,1)
n_DK2 = collect(1:N_DK2)
#n = collect(1:(N_G+N_D))
PG_DK1  = [380,350,320,370,480,900,1200]
λG_DK1 = [72,62,150,80,87,24,260]
PG_DK2  = [1100,300,380,360,320,750,600,860]
λG_DK2 = [17,44,40,37,32,5,12,235]
c_DK1 = vcat(λG_DK1)
c_DK1 = vcat(λG_DK2)
A_eq_DK1 = transpose(vcat(-ones(N_DK1)))
A_eq_DK2 = transpose(vcat(-ones(N_DK2)))
A_DK1 = Array(Diagonal(ones(N_DK1)))
A_DK2 = Array(Diagonal(ones(N_DK2)))
b_DK1 = vcat(P_G_DK1)
b_DK2 = vcat(P_G_DK2)
trans_limit = 600

# Model
ass1 = Model(with_optimizer(Gurobi.Optimizer))
@variable(ass1, y_DK1[j in n_DK1] >= 0)
@variable(ass1, y_DK2[j in n_DK2] >= 0)
@objective(ass1, Min, transpose(c_DK1)*y_DK1 + transpose(c_DK2)*y_DK2)
@constraint(ass1, generationDK1, A_DK1*y_DK1 .<= b_DK1)
@constraint(ass1, generationDK2, A_DK2*y_DK2 .<= b_DK2)
@constraint(ass1, balanceDK1[t], A_eq_DK1 *y_DK1 + en_GE[t] + en_NO[t] == trans_limit)
@constraint(ass1, balanceDK2[t], A_eq_DK2 *y_DK2 + en_SWE[t] == -trans_limit)
optimize!(ass1)

# Model output
if termination_status(ass1) == MOI.OPTIMAL
    println("Optimal solution found!\n")
    println("Generation and Demand:\n")
    for j in n_DK1
        println("$(id_G[j]): ", value.(y_DK1[j]), " MWh")
    end
    for i in n_DK2
        println("$(id_D[i]): ", value.(y_DK2[i]), " MWh")
    end
    println("\nObjective value: ", objective_value(ass1), " €")
    println("\nMarket equilibrium DK1: ", dual(balanceDK1), " €/MWh")
    println("\nMarket equilibrium DK2: ", dual(balanceDK2), " €/MWh")
    else
        error("No solution.")
end

power = Array(n)
for k in n
    power_DK1[k]=value.(y_DK1)[k]
    power_DK2[k]=value.(y_DK2)[k]
end

# Data Frames
df_G = DataFrame(Supplier=supplier,ID_G=id_G,Offer_G=P_G,Price_G=λ_G,Schedule_G=power[1:N_G],Market_G=fill(dual(balance),N_G))
df_D = DataFrame(Consumer=consumer,ID_D=id_D,Offer_D=P_D,Price_D=λ_D,Schedule_D=power[N_G+1:maximum(n)],Market_D=fill(dual(balance),N_D))
df_G.PayAsBid_G = df_G.Schedule_G.*df_G.Price_G
df_G.UniformPricing_G = df_G.Schedule_G.*df_G.Market_G
df_D.PayAsBid_D = df_D.Schedule_D.*df_D.Price_D
df_D.UniformPricing_D = df_D.Schedule_D.*df_D.Market_D

#plot(bidsSupplier.AggregatedQ,bidsSupplier.Price,w=2,t=:steppre, xlim=(0,sum(bidsSupplier.Quantity)), xlab="Quantity [MWh]", ylab="Price [EUR/MWh]", color="darkred", legend=false)
