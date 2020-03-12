using DataFrames
using Plots
using JuMP
using Gurobi
using MathOptFormat
using LinearAlgebra
using Printf
using CSV
using Dates
gr()

# Create functions

function readandclean(file)
    if file[1:11]=="consumption"
        df = CSV.read(file,header=3,datarow=4)
        df = rename!(df,:1=>"Date")
        df = select!(df, Not([:NO,:SE,:FI,:EE,:LV,:LT]))
    else
        df = CSV.read(file,header=3,datarow=4)
        df = rename!(df,:1=>"Date")
    end
    return df
end

function adjustconsumption(df)

end

function addwind(df)
    insertcols!(df,5,:WestWind₁=>df.DK1.*0.8)
    insertcols!(df,6,:WestWind₂=>df.DK1.*0.2)
    insertcols!(df,7,:EastWind₁=>df.DK2.*0.1)
    insertcols!(df,7,:EastWind₂=>df.DK2.*0.9)
end

#Read data
files=["consumption-prognosis_2019_hourly.csv","consumption-prognosis_2020_hourly.csv","wind-power-dk-prognosis_2019_hourly.csv","wind-power-dk-prognosis_2020_hourly.csv"]

#Clean files
df_consumption2019 = readandclean(files[1])
df_consumption2020 = readandclean(files[2])
df_wind2019 = readandclean(files[3])
df_wind2020 = readandclean(files[4])

#append!(df1,df2) to append two dataframes

#Adjust wind and create offers
addwind(df_wind2020)


supplier_DK1 = ["FlexiGas","FlexiGas","FlexiGas","Peako","Peako","Nuke22","CoalAtLast"]
id_GDK1 = ["G₁","G₂","G₃","G₄","G₅","G₆","G₇"]
supplier_DK2 = ["Nuke22","RoskildeCHP","RoskildeCHP","Avedøvre","Avedøvre","BlueWater","BlueWater","CoalAtLast"]
id_GDK2 = ["G₈","G₉","G₁₀","G₁₁","G₁₂","G₁₃","G₁₄","G₁₅"]
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
