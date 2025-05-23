//! Renders a glTF mesh in 2D with a custom vertex attribute.

use bevy::{
    gltf::GltfPlugin,
    prelude::*,
    reflect::TypePath,
    render::{
        mesh::{MeshVertexAttribute, MeshVertexBufferLayoutRef},
        render_resource::*,
    },
    sprite::{Material2d, Material2dKey, Material2dPlugin},
};

/// This example uses a shader source file from the assets subdirectory
const SHADER_ASSET_PATH: &str = "shaders/custom_gltf_2d.wgsl";

/// This vertex attribute supplies barycentric coordinates for each triangle.
///
/// Each component of the vector corresponds to one corner of a triangle. It's
/// equal to 1.0 in that corner and 0.0 in the other two. Hence, its value in
/// the fragment shader indicates proximity to a corner or the opposite edge.
const ATTRIBUTE_BARYCENTRIC: MeshVertexAttribute =
    MeshVertexAttribute::new("Barycentric", 2137464976, VertexFormat::Float32x3);

fn main() {
    App::new()
        .insert_resource(AmbientLight {
            color: Color::WHITE,
            brightness: 1.0 / 5.0f32,
            ..default()
        })
        .add_plugins((
            DefaultPlugins.set(
                GltfPlugin::default()
                    // Map a custom glTF attribute name to a `MeshVertexAttribute`.
                    // The glTF file used here has an attribute name with *two*
                    // underscores: __BARYCENTRIC
                    // One is stripped to do the comparison here.
                    .add_custom_vertex_attribute("_BARYCENTRIC", ATTRIBUTE_BARYCENTRIC),
            ),
            Material2dPlugin::<CustomMaterial>::default(),
        ))
        .add_systems(Startup, setup)
        .run();
}

fn setup(
    mut commands: Commands,
    asset_server: Res<AssetServer>,
    mut materials: ResMut<Assets<CustomMaterial>>,
) {
    // Add a mesh loaded from a glTF file. This mesh has data for `ATTRIBUTE_BARYCENTRIC`.
    let mesh = asset_server.load(
        GltfAssetLabel::Primitive {
            mesh: 0,
            primitive: 0,
        }
        .from_asset("models/barycentric/barycentric.gltf"),
    );
    commands.spawn((
        Mesh2d(mesh),
        MeshMaterial2d(materials.add(CustomMaterial {})),
        Transform::from_scale(150.0 * Vec3::ONE),
    ));

    commands.spawn(Camera2d);
}

/// This custom material uses barycentric coordinates from
/// `ATTRIBUTE_BARYCENTRIC` to shade a white border around each triangle. The
/// thickness of the border is animated using the global time shader uniform.
#[derive(Asset, TypePath, AsBindGroup, Debug, Clone)]
struct CustomMaterial {}

impl Material2d for CustomMaterial {
    fn vertex_shader() -> ShaderRef {
        SHADER_ASSET_PATH.into()
    }
    fn fragment_shader() -> ShaderRef {
        SHADER_ASSET_PATH.into()
    }

    fn specialize(
        descriptor: &mut RenderPipelineDescriptor,
        layout: &MeshVertexBufferLayoutRef,
        _key: Material2dKey<Self>,
    ) -> Result<(), SpecializedMeshPipelineError> {
        let vertex_layout = layout.0.get_layout(&[
            Mesh::ATTRIBUTE_POSITION.at_shader_location(0),
            Mesh::ATTRIBUTE_COLOR.at_shader_location(1),
            ATTRIBUTE_BARYCENTRIC.at_shader_location(2),
        ])?;
        descriptor.vertex.buffers = vec![vertex_layout];
        Ok(())
    }
}
